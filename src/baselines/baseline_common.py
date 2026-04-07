import collections
import io
import json
import math
import os
import pickle
import re
import sqlite3
import statistics
import sys
import traceback
import warnings

from contextlib import redirect_stdout
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import matplotlib
import numpy as np
import pandas as pd
import pm4py
from dotenv import load_dotenv
from langchain_community.vectorstores import FAISS
from langchain_openai import ChatOpenAI, OpenAIEmbeddings

warnings.filterwarnings("ignore", category=UserWarning)
matplotlib.use("Agg")

MODEL_NAME = "gpt-4o-mini"
MAX_RETRIES = 2
MANUAL_TOP_K = 5


@dataclass(frozen=True)
class BaselineSpec:
    label: str
    result_col: str
    include_log_summary: bool
    include_manual_rag: bool


class LogUtils:
    GREEN = "\033[92m"
    CYAN = "\033[96m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    RESET = "\033[0m"
    BOLD = "\033[1m"

    @staticmethod
    def header(title: str):
        print(f"\n{LogUtils.CYAN}{'=' * 80}\n[ {title} ]\n{'=' * 80}{LogUtils.RESET}")

    @staticmethod
    def info(key: str, value: Any, truncate_len: int = 400):
        text = str(value)
        if truncate_len and len(text) > truncate_len:
            text = text[:truncate_len] + f"... (total {len(str(value))} chars)"
        print(f"   - {LogUtils.BOLD}{key}:{LogUtils.RESET} {text}")

    @staticmethod
    def code_snippet(title: str, code: str):
        print(f"   - {LogUtils.BOLD}{title}:{LogUtils.RESET}\n{LogUtils.YELLOW}{code}{LogUtils.RESET}")

    @staticmethod
    def error(msg: str, details: str = ""):
        print(f"\n{LogUtils.RED}✖ [ERROR]{LogUtils.RESET} {msg}")
        if details:
            print(details)


class DualLogger:
    def __init__(self, filepath: str):
        self.terminal = sys.stdout
        self.log = open(filepath, "w", encoding="utf-8")
        self.ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

    def write(self, message: str):
        self.terminal.write(message)
        clean_message = self.ansi_escape.sub("", message)
        self.log.write(clean_message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()


class ManualRAGManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        self.vector_db = None
        if os.path.exists(db_path):
            self.vector_db = FAISS.load_local(
                db_path,
                self.embeddings,
                allow_dangerous_deserialization=True,
            )
            print(f"[RAG] ✅ Loaded manual DB from {db_path}")
        else:
            raise FileNotFoundError(f"Manual RAG DB not found: {db_path}")

    def search(self, query: str, category: str, k: int = MANUAL_TOP_K):
        if self.vector_db is None:
            return []
        try:
            candidates = self.vector_db.similarity_search_with_score(query, k=max(k * 2, k))
        except Exception as exc:
            print(f"[RAG] ❌ Search Error: {exc}")
            return []

        wanted_ocel = category == "ocel"
        filtered = []
        for doc, score in candidates:
            meta = getattr(doc, "metadata", {}) or {}
            is_object_centric = meta.get("is_object_centric")
            if is_object_centric is None or bool(is_object_centric) == wanted_ocel:
                filtered.append((doc, score))
            if len(filtered) >= k:
                break
        return filtered[:k]


def load_json_file_safe(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def preview_list(values: Any, limit: int = 20) -> str:
    if not isinstance(values, list) or not values:
        return "N/A"
    rendered = [str(v) for v in values[:limit]]
    suffix = " ..." if len(values) > limit else ""
    return ", ".join(rendered) + suffix


def preview_dict(values: Any, limit: int = 5) -> str:
    if not isinstance(values, dict) or not values:
        return "N/A"
    rendered = [f"{key}={value}" for key, value in list(values.items())[:limit]]
    suffix = " ..." if len(values) > limit else ""
    return ", ".join(rendered) + suffix


def clean_and_parse_json(content: str) -> Dict[str, Any]:
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        content = content.replace("```json", "").replace("```", "").strip()
        match = re.search(r"(\{.*\})", content, re.DOTALL)
        if match:
            return json.loads(match.group(1))
        raise ValueError(f"JSON Parsing Failed. Raw content: {content[:500]}...")


def get_query_text(row: pd.Series) -> Optional[str]:
    for col in ["query", "질의", "question", "Query"]:
        if col in row and pd.notna(row.get(col)):
            text = str(row.get(col)).strip()
            if text:
                return text
    return None


def get_query_no(row: pd.Series, default: Any = "") -> str:
    for col in ["no", "No", "count no", "Count No"]:
        if col in row and pd.notna(row.get(col)):
            return str(row.get(col)).strip()
    return str(default).strip()


def get_query_category(row: pd.Series) -> str:
    raw = str(row.get("category", "ocel")).strip().lower()
    return "general" if raw == "general" else "ocel"


def get_query_type(row: pd.Series) -> str:
    for col in ["query type", "Query Type"]:
        if col in row and pd.notna(row.get(col)):
            return str(row.get(col)).strip()
    return ""


def get_answer_format(row: pd.Series) -> str:
    for col in ["Answer format", "answer format"]:
        if col in row and pd.notna(row.get(col)):
            return str(row.get(col)).strip()
    return ""


def get_query_interpretation(row: pd.Series) -> str:
    for col in ["해석", "interpretation", "Interpretation"]:
        if col in row and pd.notna(row.get(col)):
            return str(row.get(col)).strip()
    return ""


def parse_target_nos_env(env_var: str = "RUN_QUERY_NOS") -> List[str]:
    raw = os.getenv(env_var, "").strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def slugify(text: Any) -> str:
    value = re.sub(r"[^a-zA-Z0-9_]+", "_", str(text).strip().lower())
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "query"


def json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [json_ready(v) for v in value]
    if isinstance(value, set):
        return [json_ready(v) for v in sorted(value, key=lambda x: str(x))]
    if isinstance(value, pd.DataFrame):
        return [json_ready(rec) for rec in value.to_dict(orient="records")]
    if isinstance(value, pd.Series):
        return [json_ready(v) for v in value.tolist()]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, Path):
        return str(value)
    return value


def clip_text(text: Any, limit: int = 900) -> str:
    value = str(text or "").strip()
    if not value:
        return "N/A"
    value = re.sub(r"\s+", " ", value)
    if len(value) <= limit:
        return value
    return value[:limit] + f"... (truncated, total {len(value)} chars)"


def normalize_free_text(text: Any) -> str:
    lowered = str(text).strip().lower()
    lowered = re.sub(r"[_/()\-]+", " ", lowered)
    lowered = re.sub(r"[^a-z0-9\s]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def normalize_bpi_activity_label(label: str) -> str:
    normalized = normalize_free_text(label)
    return re.sub(r"^[a-z]\s+", "", normalized).strip()


def save_json_file(path: str, payload: Any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(json_ready(payload), f, ensure_ascii=False, indent=2)


class ProjectConfig:
    def __init__(self, script_path: str):
        self.src_dir = Path(script_path).resolve().parent
        self.repo_root = self.src_dir.parent.parent
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.current_script_name = os.path.basename(script_path)
        self.script_name = self.current_script_name.replace(".py", "")
        self.file_prefix = f"{self.timestamp}_{self.script_name}"

        self.artifacts_dir = str(self.repo_root / "artifacts" / self.file_prefix)
        self.base_dir = self.artifacts_dir
        self.output_rel_dir = "output"
        self.output_dir = os.path.join(self.artifacts_dir, self.output_rel_dir)
        self.eval_dir = self.artifacts_dir
        self.plan_dir = os.path.join(self.artifacts_dir, "plans")
        self.code_dir = os.path.join(self.artifacts_dir, "codes")
        self.log_dir = self.artifacts_dir
        self.log_file = os.path.join(self.log_dir, f"{self.file_prefix}_log.txt")

        self.query_file = str(
            self.repo_root / "query" / "benchmark_120_complex_pm_queries.xlsx"
        )
        self.input_xes_general = str(self.repo_root / "data" / "BPI Challenge 2017.xes")
        self.input_sqlite_ocel = str(self.repo_root / "data" / "order-management.sqlite")
        self.general_schema_file = str(self.repo_root / "data" / "BPI Challenge 2017_data_schema.json")
        self.ocel_schema_file = str(self.repo_root / "data" / "order-management_data_schema.json")
        self.general_abstraction_file = str(self.repo_root / "context" / "bpi-challenge-2017_pm_abstractions.json")
        self.ocel_abstraction_file = str(self.repo_root / "context" / "order-management_pm_abstractions.json")
        self.manual_rag_db = str(self.repo_root / "resources" / "pm4py_faiss_db")

    def ensure_dirs(self):
        for directory in [
            self.artifacts_dir,
            self.base_dir,
            self.output_dir,
            self.eval_dir,
            self.plan_dir,
            self.code_dir,
            self.log_dir,
        ]:
            os.makedirs(directory, exist_ok=True)
        try:
            os.chdir(self.base_dir)
        except Exception:
            pass

    def get_output_csv_path(self) -> str:
        return os.path.join(self.eval_dir, f"{self.file_prefix}.csv")

    def get_analysis_plan_path(self, query_no: Any) -> str:
        return os.path.join(self.plan_dir, f"query_{slugify(query_no)}_analysis_plan.json")

    def get_code_path(self, query_no: Any) -> str:
        return os.path.join(self.code_dir, f"query_{slugify(query_no)}_final_code.py")

    def get_result_json_path(self, query_no: Any) -> str:
        return os.path.join(self.output_dir, f"query_{slugify(query_no)}_result.json")


class SingleAgentBaselineRunner:
    def __init__(self, script_path: str, spec: BaselineSpec):
        self.spec = spec
        self.cfg = ProjectConfig(script_path)
        self.cfg.ensure_dirs()
        load_dotenv(self.cfg.repo_root / ".env")
        if not os.getenv("OPENAI_API_KEY"):
            raise ValueError("OPENAI_API_KEY가 .env 파일에 설정되지 않았습니다.")

        self.llm = ChatOpenAI(model=MODEL_NAME, temperature=0, max_retries=3, timeout=180)
        self.manual_rag = ManualRAGManager(self.cfg.manual_rag_db) if spec.include_manual_rag else None
        self._xes_log = None
        self._xes_df = None
        self._ocel_log = None
        self._dataset_context_cache: Dict[str, str] = {}
        self._runtime_profile_cache: Dict[str, str] = {}
        self._summary_cache: Dict[str, str] = {}
        self._schema_payload_cache: Dict[str, Dict[str, Any]] = {}
        self._general_abstraction_context: Optional[str] = None
        self._ocel_abstraction_context: Optional[str] = None

        print(
            f"[Init] LLM initialized. {self.spec.label} | model={MODEL_NAME} "
            f"| log_summary={self.spec.include_log_summary} | manual_rag={self.spec.include_manual_rag}"
        )

    def load_query_file(self, path: str) -> pd.DataFrame:
        suffix = Path(path).suffix.lower()
        if suffix in [".xlsx", ".xls"]:
            xls = pd.ExcelFile(path)
            frames = []
            for sheet_name in xls.sheet_names:
                temp = pd.read_excel(path, sheet_name=sheet_name)
                lowered = {str(c).strip().lower() for c in temp.columns}
                if any(c in lowered for c in ["query", "질의", "question"]):
                    temp["__sheet_name__"] = sheet_name
                    frames.append(temp)
            if frames:
                return pd.concat(frames, ignore_index=True)
            return pd.read_excel(path)
        return pd.read_csv(path)

    def load_xes_once(self):
        if self._xes_log is None:
            self._xes_log = pm4py.read_xes(self.cfg.input_xes_general)
            self._xes_df = pm4py.convert_to_dataframe(self._xes_log)
        return self._xes_log

    def load_xes_df_once(self):
        if self._xes_df is None:
            self.load_xes_once()
        return self._xes_df

    def load_ocel_once(self):
        if self._ocel_log is None:
            self._ocel_log = pm4py.read_ocel2_sqlite(self.cfg.input_sqlite_ocel)
        return self._ocel_log

    def get_dataset_schema(self, category: str) -> Dict[str, Any]:
        if category not in self._schema_payload_cache:
            schema_path = self.cfg.general_schema_file if category == "general" else self.cfg.ocel_schema_file
            self._schema_payload_cache[category] = load_json_file_safe(schema_path)
        return self._schema_payload_cache[category]

    def build_runtime_profile(self, category: str) -> str:
        if category in self._runtime_profile_cache:
            return self._runtime_profile_cache[category]

        try:
            if category == "general":
                log_df = self.load_xes_df_once().copy()
                case_count = int(log_df["case:concept:name"].nunique()) if "case:concept:name" in log_df.columns else 0
                event_count = int(len(log_df))
                activity_counts = (
                    log_df["concept:name"].value_counts().head(10).to_dict()
                    if "concept:name" in log_df.columns else {}
                )
                runtime_profile = (
                    "[Runtime Log Profile]\n"
                    f"- Cases: {case_count}\n"
                    f"- Events: {event_count}\n"
                    f"- Top activities: {json.dumps(activity_counts, ensure_ascii=False)}"
                )
            else:
                ocel = self.load_ocel_once()
                events = ocel.events.copy() if hasattr(ocel, "events") else pd.DataFrame()
                objects = ocel.objects.copy() if hasattr(ocel, "objects") else pd.DataFrame()
                relations = ocel.relations.copy() if hasattr(ocel, "relations") else pd.DataFrame()
                object_type_counts = (
                    objects["ocel:type"].astype(str).value_counts().head(10).to_dict()
                    if "ocel:type" in objects.columns else {}
                )
                activity_counts = (
                    events["ocel:activity"].astype(str).value_counts().head(10).to_dict()
                    if "ocel:activity" in events.columns else {}
                )
                runtime_profile = (
                    "[Runtime Log Profile]\n"
                    f"- Event rows: {len(events)}\n"
                    f"- Object rows: {len(objects)}\n"
                    f"- Relation rows: {len(relations)}\n"
                    f"- Object type distribution: {json.dumps(object_type_counts, ensure_ascii=False)}\n"
                    f"- Top activities: {json.dumps(activity_counts, ensure_ascii=False)}"
                )
        except Exception as exc:
            runtime_profile = f"[Runtime Log Profile]\n- Unavailable: {exc}"

        self._runtime_profile_cache[category] = runtime_profile
        return runtime_profile

    def build_ocel_abstraction_context(self) -> str:
        if self._ocel_abstraction_context is not None:
            return self._ocel_abstraction_context

        abstraction = load_json_file_safe(self.cfg.ocel_abstraction_file)
        if not abstraction:
            self._ocel_abstraction_context = (
                "[OCEL Abstraction Context]\n"
                f"- Not available at: {self.cfg.ocel_abstraction_file}"
            )
            return self._ocel_abstraction_context

        object_profiles = abstraction.get("object_type_profiles", {})
        profile_lines = []
        if isinstance(object_profiles, dict):
            for object_type, profile in list(object_profiles.items())[:6]:
                if not isinstance(profile, dict):
                    continue
                profile_lines.append(
                    f"- {object_type}: object_count={profile.get('object_count', 'N/A')}, "
                    f"flattened_rows={profile.get('flattened_rows', 'N/A')}, "
                    f"flattened_cases={profile.get('flattened_cases', 'N/A')}, "
                    f"top_flattened_activities={preview_dict(profile.get('top_flattened_activities', {}), limit=4)}"
                )

        profile_block = "\n".join(profile_lines) if profile_lines else "- Object-type profiles: N/A"
        self._ocel_abstraction_context = f"""
[OCEL Abstraction Context]
- Source abstraction file: {self.cfg.ocel_abstraction_file}
- Source dataset: {abstraction.get('source_dataset', 'order-management.json')}
- Primary abstraction object type: {abstraction.get('target_object_type', 'orders')}
- Available object types: {preview_list(abstraction.get('available_object_types'))}
- Process structure hints: {clip_text(abstraction.get('process_structure_hints', ''), limit=1000)}
- OCDFG summary excerpt: {clip_text(abstraction.get('ocdfg_summary', ''), limit=1200)}
- OCPN summary excerpt: {clip_text(abstraction.get('ocpn_summary', ''), limit=900)}
- Primary-object feature summary excerpt: {clip_text(abstraction.get('features_summary', ''), limit=900)}
- Primary-object variant summary excerpt: {clip_text(abstraction.get('variants_summary', ''), limit=900)}
- Per-object-type flattened hints:
{profile_block}
""".strip()
        return self._ocel_abstraction_context

    def build_general_abstraction_context(self) -> str:
        if self._general_abstraction_context is not None:
            return self._general_abstraction_context

        abstraction = load_json_file_safe(self.cfg.general_abstraction_file)
        if not abstraction:
            self._general_abstraction_context = (
                "[General Abstraction Context]\n"
                f"- Not available at: {self.cfg.general_abstraction_file}"
            )
            return self._general_abstraction_context

        metrics = abstraction.get("summary_metrics", {})
        self._general_abstraction_context = f"""
[General Abstraction Context]
- Source abstraction file: {self.cfg.general_abstraction_file}
- Source dataset: {abstraction.get('source_dataset', self.cfg.input_xes_general)}
- Primary case notion: {abstraction.get('target_object_type', 'case:concept:name')}
- Available case notions: {preview_list(abstraction.get('available_object_types'))}
- Process structure hints: {clip_text(abstraction.get('process_structure_hints', ''), limit=1000)}
- DFG summary excerpt: {clip_text(abstraction.get('ocdfg_summary', ''), limit=1200)}
- Petri net summary excerpt: {clip_text(abstraction.get('ocpn_summary', ''), limit=900)}
- Log feature summary excerpt: {clip_text(abstraction.get('features_summary', ''), limit=900)}
- Variant summary excerpt: {clip_text(abstraction.get('variants_summary', ''), limit=900)}
- Log attribute summary excerpt: {clip_text(abstraction.get('attributes_summary', ''), limit=900)}
- Summary metrics: {clip_text(json.dumps(metrics, ensure_ascii=False), limit=900)}
""".strip()
        return self._general_abstraction_context

    def resolve_general_activity_aliases(self, query: str, interpretation: str, schema: Dict[str, Any]) -> List[Dict[str, str]]:
        combined = normalize_free_text(f"{query} {interpretation}")
        if not combined:
            return []

        candidates: Dict[str, List[str]] = {}
        for activity in schema.get("activities", []):
            normalized = normalize_bpi_activity_label(str(activity))
            if len(normalized) < 4:
                continue
            candidates.setdefault(normalized, []).append(str(activity))

        aliases: List[Dict[str, str]] = []
        for phrase, matches in candidates.items():
            if len(matches) != 1:
                continue
            if f" {phrase} " not in f" {combined} ":
                continue
            exact_activity = matches[0]
            if exact_activity.lower() in str(query).lower():
                continue
            aliases.append({"query_phrase": phrase, "dataset_activity": exact_activity})
        return aliases[:8]

    def build_query_requirements(self, query_no: str, category: str, query: str, interpretation: str) -> str:
        schema = self.get_dataset_schema(category)
        lower = f"{query} {interpretation}".lower()
        lines = ["[Query-Specific Requirements]"]
        lines.append(f"- Query no: {query_no or 'N/A'}")
        if interpretation:
            lines.append(f"- Interpretation hint: {interpretation}")

        if category == "general":
            aliases = self.resolve_general_activity_aliases(query, interpretation, schema)
            lines.append("- Working view: case-centric event log")
            lines.append(
                f"- Requires reference model: {'yes' if 'reference petri net' in lower or 'given a reference' in lower else 'no'}"
            )
            if aliases:
                rendered = "; ".join(
                    f"'{item['query_phrase']}' -> '{item['dataset_activity']}'" for item in aliases
                )
                lines.append(f"- Resolved activity aliases from query to exact dataset labels: {rendered}")
        else:
            object_types = [str(item) for item in schema.get("object_types", [])]
            mentioned_object_types = [item for item in object_types if item in lower]
            mentions_raw = "raw ocel" in lower
            mentions_flatten = any(token in lower for token in ["flattened ", "flatten ", "case notion", "treated as a case"])
            if mentions_raw and mentions_flatten:
                scope = "mixed raw OCEL + flattened case-centric view"
            elif mentions_flatten:
                scope = "flattened case-centric view"
            elif mentions_raw:
                scope = "raw OCEL"
            else:
                scope = "OCEL analysis (scope inferred from query)"

            flatten_object_type = ""
            for object_type in object_types:
                patterns = [
                    f"flattened {object_type} view",
                    f"using {object_type} as the case notion",
                    f"each {object_type} object is treated as a case",
                    f"full flattened {object_type} view",
                ]
                if any(pattern in lower for pattern in patterns):
                    flatten_object_type = object_type
                    break

            requires_reference_model = any(
                pattern in lower for pattern in [
                    "reference petri net",
                    "reference object-centric petri net",
                    "reference oc-petri net",
                    "given a reference",
                ]
            )
            requires_joint_filter = any(
                pattern in lower for pattern in [
                    "linked to at least one",
                    "simultaneously linked to both",
                    "both orders and customers",
                    "both orders and items",
                    "both items and customers",
                    "both customers and employees",
                ]
            )

            lines.append(f"- Working scope: {scope}")
            if flatten_object_type:
                lines.append(f"- Flattened primary object type: {flatten_object_type}")
            elif mentions_flatten:
                lines.append("- Flattened primary object type: unresolved from query text; resolve from exact schema values only")
            lines.append(f"- Mentioned object types from schema: {preview_list(mentioned_object_types, limit=10)}")
            lines.append(f"- Requires reference model: {'yes' if requires_reference_model else 'no'}")
            lines.append(f"- Requires joint-event filtering: {'yes' if requires_joint_filter else 'no'}")
            lines.append(f"- Requires propagated restricted OCEL: {'yes' if 'propagate the filter' in lower else 'no'}")

        return "\n".join(lines)

    def build_data_context(self, category: str, query_requirements: str = "") -> str:
        if category not in self._dataset_context_cache:
            schema = self.get_dataset_schema(category)
            runtime_profile = self.build_runtime_profile(category)

            if category == "general":
                context = f"""
[Dataset Context: BPI Challenge 2017]
- Execution input: {self.cfg.input_xes_general}
- Schema source: {self.cfg.general_schema_file}
- General abstraction source: {self.cfg.general_abstraction_file}
- Format: {schema.get('format', 'XES')}
- Exact activity column: concept:name
- Exact timestamp column: time:timestamp
- Exact resource column: org:resource
- Exact case id column: case:concept:name
- Event attributes: {preview_list(schema.get('event_attributes'))}
- Case/object attributes exposed in the dataframe: {preview_list(schema.get('object_attributes'))}
- Known activity names: {preview_list(schema.get('activities'), limit=30)}
- Important naming rule: activity names are exact strings with prefixes such as A_, O_, W_. Never normalize them to simplified names.
- If the benchmark query uses a human-friendly activity phrase, resolve it to the closest unique exact activity label from the schema before writing code.
- Use PM4Py event-log APIs or a dataframe created via pm4py.convert_to_dataframe(event_log).
- Never use OCEL accessors like ocel.events / ocel.objects / ocel.relations for this dataset.

{self.build_general_abstraction_context()}

{runtime_profile}
""".strip()
            else:
                context = f"""
[Dataset Context: order-management OCEL]
- Execution input: {self.cfg.input_sqlite_ocel}
- Semantic schema source: {self.cfg.ocel_schema_file}
- OCEL abstraction source: {self.cfg.ocel_abstraction_file}
- Format: {schema.get('format', 'OCEL 2.0')}
- PM4Py event columns: ocel:eid, ocel:timestamp, ocel:activity
- PM4Py object columns: ocel:oid, role, weight, price, ocel:type
- PM4Py relation columns: ocel:eid, ocel:oid, ocel:qualifier, ocel:activity, ocel:timestamp, ocel:type
- Object types present in the dataset: {preview_list(schema.get('object_types'))}
- Activity names present in the dataset: {preview_list(schema.get('activities'), limit=30)}
- Object attributes present in the dataset: {preview_list(schema.get('object_attributes'))}
- Event attributes described by the semantic schema: {preview_list(schema.get('event_attributes'))}
- Important naming rule: the real order-management activities are lowercase strings with spaces.
- Never invent object types or flattened views that are not present in this dataset context.
- Allowed flattened primary object types: {preview_list(schema.get('object_types'))}
- For flattened views, use pm4py.ocel_flattening(ocel_or_restricted_ocel, object_type) where object_type is the exact dataset string such as "orders" or "packages", and then treat the result as a case-centric event log with columns like case:concept:name, concept:name, time:timestamp.
- Use OCEL accessors only: ocel.events, ocel.objects, ocel.relations.

{self.build_ocel_abstraction_context()}

{runtime_profile}
""".strip()
            self._dataset_context_cache[category] = context

        base_context = self._dataset_context_cache[category]
        if query_requirements:
            return f"{base_context}\n\n{query_requirements}"
        return base_context

    def build_log_summary_context(self, category: str) -> str:
        if not self.spec.include_log_summary:
            return ""
        if category in self._summary_cache:
            return self._summary_cache[category]

        if category == "general":
            abstraction = load_json_file_safe(self.cfg.general_abstraction_file)
            metrics = abstraction.get("summary_metrics", {})
            context = (
                "[Log Summary]\n"
                f"- source_dataset: {abstraction.get('source_dataset', self.cfg.input_xes_general)}\n"
                f"- target_object_type: {abstraction.get('target_object_type', 'case:concept:name')}\n"
                f"- available_object_types: {preview_list(abstraction.get('available_object_types'))}\n"
                f"- process_structure_hints: {clip_text(abstraction.get('process_structure_hints', ''), limit=1000)}\n"
                f"- variants_summary: {clip_text(abstraction.get('variants_summary', ''), limit=1200)}\n"
                f"- ocdfg_summary: {clip_text(abstraction.get('ocdfg_summary', ''), limit=1200)}\n"
                f"- ocpn_summary: {clip_text(abstraction.get('ocpn_summary', ''), limit=1200)}\n"
                f"- features_summary: {clip_text(abstraction.get('features_summary', ''), limit=1200)}\n"
                f"- attributes_summary: {clip_text(abstraction.get('attributes_summary', ''), limit=1200)}\n"
                f"- summary_metrics: {clip_text(json.dumps(metrics, ensure_ascii=False), limit=1200)}"
            )
        else:
            abstraction = load_json_file_safe(self.cfg.ocel_abstraction_file)
            context = (
                "[Log Summary]\n"
                f"- source_dataset: {abstraction.get('source_dataset', 'order-management.json')}\n"
                f"- target_object_type: {abstraction.get('target_object_type', 'N/A')}\n"
                f"- available_object_types: {preview_list(abstraction.get('available_object_types'))}\n"
                f"- process_structure_hints: {clip_text(abstraction.get('process_structure_hints', ''), limit=1000)}\n"
                f"- variants_summary: {clip_text(abstraction.get('variants_summary', ''), limit=1200)}\n"
                f"- ocdfg_summary: {clip_text(abstraction.get('ocdfg_summary', ''), limit=1200)}\n"
                f"- ocpn_summary: {clip_text(abstraction.get('ocpn_summary', ''), limit=1200)}"
            )

        self._summary_cache[category] = context
        return context

    def build_benchmark_context(self, query_type: str, answer_format: str, interpretation: str = "") -> str:
        lines = [f"- Benchmark query type: {query_type or 'Unknown'}"]
        if interpretation:
            lines.append(f"- Query interpretation hint: {interpretation}")
        try:
            parsed = json.loads(answer_format) if answer_format else None
        except Exception:
            parsed = None

        if isinstance(parsed, dict):
            lines.append(f"- Expected result type: {parsed.get('result_type', 'N/A')}")
            lines.append(f"- Expected view: {parsed.get('view', 'N/A')}")
            result_schema = parsed.get("result_schema")
            if isinstance(result_schema, dict) and result_schema:
                lines.append(f"- Expected result schema keys: {', '.join(result_schema.keys())}")
        elif answer_format:
            lines.append(f"- Expected answer format: {answer_format}")
        return "[Benchmark Metadata]\n" + "\n".join(lines)

    def build_manual_context(self, query: str, category: str, prior_analysis: str = "", error_text: str = "") -> str:
        if not self.spec.include_manual_rag or self.manual_rag is None:
            return ""
        search_query = "\n".join(part for part in [query, prior_analysis, error_text] if part).strip()
        docs = self.manual_rag.search(search_query, category=category, k=MANUAL_TOP_K)
        if not docs:
            return ""
        rendered = []
        for idx, (doc, score) in enumerate(docs, start=1):
            meta = getattr(doc, "metadata", {}) or {}
            title = meta.get("function_name") or meta.get("section_title") or f"manual_{idx}"
            snippet = doc.page_content.strip()
            if len(snippet) > 1200:
                snippet = snippet[:1200] + f"... (total {len(doc.page_content)} chars)"
            rendered.append(f"[{idx}] {title} | score={score:.4f}\n{snippet}")
        return "\n\n".join(rendered)

    def build_retry_feedback(self, state: Dict[str, Any], category: str) -> str:
        if not state.get("error"):
            return ""
        previous_code = state.get("python_code", "")
        previous_plan = state.get("analysis_plan", "")
        category_hint = (
            "Stay within event_log, the exact BPI 2017 columns, and exact dataset activity labels."
            if category == "general"
            else "Stay within ocel.events / ocel.objects / ocel.relations, exact OCEL columns, and keep raw OCEL separate from flattened views."
        )
        return f"""
[Retry Feedback]
- Previous error:
{state.get('error')}

- Previous analysis plan:
{previous_plan}

- Previous code:
```python
{previous_code}
```

- Repair directive:
Study the failure carefully, do not repeat the same mistake, and explicitly change the approach where needed.
{category_hint}
""".strip()

    def build_prompt(
        self,
        query_no: str,
        query: str,
        category: str,
        query_type: str,
        answer_format: str,
        interpretation: str,
        state: Dict[str, Any],
    ) -> str:
        query_requirements = self.build_query_requirements(query_no, category, query, interpretation)
        benchmark_context = self.build_benchmark_context(query_type, answer_format, interpretation)
        context_blocks = [self.build_data_context(category, query_requirements=query_requirements), benchmark_context]
        if self.spec.include_log_summary:
            context_blocks.append(self.build_log_summary_context(category))
        manual_context = self.build_manual_context(
            query=query,
            category=category,
            prior_analysis=state.get("analysis_plan", ""),
            error_text=state.get("error", ""),
        )
        if manual_context:
            context_blocks.append("[PM4Py Manual Snippets]\n" + manual_context)
        retry_feedback = self.build_retry_feedback(state, category)
        if retry_feedback:
            context_blocks.append(retry_feedback)

        dataset_rules = """
[Category Rules: general]
- The input log is a case-centric XES event log.
- Use exact columns: concept:name, time:timestamp, org:resource, case:concept:name.
- Never use OCEL accessors.
- Resolve human-friendly activity phrases to exact BPI dataset labels when query-specific alias hints are given.
- Use PM4Py event-log APIs or a dataframe created via pm4py.convert_to_dataframe(event_log).

[Category Rules: ocel]
- The input log is order-management OCEL sqlite.
- Use exact columns: ocel:eid, ocel:timestamp, ocel:activity, ocel:oid, ocel:type, ocel:qualifier.
- Only use ocel.events, ocel.objects, ocel.relations.
- Activity and object-type strings must match the real order-management dataset exactly.
- If a flattened view is needed, explicitly use pm4py.ocel_flattening(ocel_or_restricted_ocel, "orders") or pm4py.ocel_flattening(ocel_or_restricted_ocel, object_type="orders") with the exact dataset object type.
- If the query mixes raw OCEL and flattened reasoning, keep raw `ocel:*` columns separate from case-centric columns like concept:name.
- If the query says "propagate the filter", first restrict the raw OCEL and only then flatten or discover on the restricted OCEL.
""".strip()

        reference_patterns = """
[Reference Pattern: general]
```python
def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    start_activities = log_df.groupby("case:concept:name")["concept:name"].first().value_counts().to_dict()
    end_activities = log_df.groupby("case:concept:name")["concept:name"].last().value_counts().to_dict()
    final_answer = {"start_activity": start_activities, "end_activity": end_activities}
    print(json.dumps(final_answer, ensure_ascii=False))
```

[Reference Pattern: general_dfg]
```python
def main():
    event_log = ACTIVE_LOG
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)
    png_path = "output/dfg_visualization.png"
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, png_path)
    print(f"OUTPUT_FILE_LOCATION: {png_path}")
    with open("output/dfg.pkl", "wb") as f:
        pickle.dump(dfg, f)
    print("OUTPUT_FILE_LOCATION: output/dfg.pkl")
    total = sum(dfg.values()) if dfg else 0
    top_edges = [
        {"source": src, "target": dst, "count": count, "share": (count / total if total else 0.0)}
        for (src, dst), count in sorted(dfg.items(), key=lambda x: x[1], reverse=True)[:10]
    ]
    final_answer = {"top_edges": top_edges}
    print(json.dumps(final_answer, ensure_ascii=False))
```

[Reference Pattern: ocel]
```python
def main():
    ocel = ACTIVE_LOG
    ocdfg = pm4py.discover_ocdfg(ocel)
    path = "output/example_ocdfg.png"
    pm4py.save_vis_ocdfg(ocdfg, path, annotation="frequency")
    print(f"OUTPUT_FILE_LOCATION: {path}")
    edge_tables = ocdfg["edges"]["event_couples"]
    top_rows = []
    for object_type, edge_map in edge_tables.items():
        for (src, dst), linked_pairs in edge_map.items():
            top_rows.append({
                "object_type": object_type,
                "source": src,
                "target": dst,
                "count": len(linked_pairs),
            })
    top_rows = sorted(top_rows, key=lambda row: row["count"], reverse=True)[:10]
    total_nodes = len(ocdfg["activities"])
    total_edges = sum(len(edge_map) for edge_map in edge_tables.values())
    final_answer = {
        "graph_type": "ocdfg",
        "total_nodes": total_nodes,
        "total_edges": total_edges,
        "top_edges": top_rows,
    }
    with open("output/ocdfg.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/ocdfg.json")
    print(json.dumps(final_answer, ensure_ascii=False))
```

[Reference Pattern: ocel_ocpn]
```python
def main():
    ocel = ACTIVE_LOG
    ocpn = pm4py.discover_oc_petri_net(ocel)
    png_path = "output/ocpn.png"
    pm4py.save_vis_ocpn(ocpn, png_path)
    print(f"OUTPUT_FILE_LOCATION: {png_path}")
    with open("output/ocpn.pkl", "wb") as f:
        pickle.dump(ocpn, f)
    print("OUTPUT_FILE_LOCATION: output/ocpn.pkl")
    per_object_type = {}
    for object_type, (net, initial_marking, final_marking) in ocpn["petri_nets"].items():
        per_object_type[object_type] = {
            "places": len(net.places),
            "transitions": len(net.transitions),
            "arcs": len(net.arcs),
        }
    final_answer = {"graph_type": "ocpn", "per_object_type": per_object_type}
    print(json.dumps(final_answer, ensure_ascii=False))
```
""".strip()

        python_contract = """
[Python Contract]
1. Return JSON only with keys "analysis_plan" and "python_code".
2. "python_code" must be a complete executable Python script with def main():.
3. Inside main():
   - If category is general: start with event_log = ACTIVE_LOG
   - If category is ocel: start with ocel = ACTIVE_LOG
4. Do not read the source log file inside python_code.
5. Save all generated artifacts under output/...
6. After saving each artifact, print OUTPUT_FILE_LOCATION: <path>
7. The last non-empty printed line MUST be json.dumps(final_answer, ensure_ascii=False)
8. final_answer must be JSON-serializable and reflect the query plus Answer format as closely as possible.
8a. If you save a .json artifact, it must also be JSON-serializable. Do not json.dump raw PM4Py objects that contain sets, tuples, Petri net objects, or other non-serializable values.
9. Use only standard library plus already available pm4py, pandas as pd, numpy as np, json, os, pickle, math, statistics, collections.
10. Avoid printing huge objects. Keep diagnostic prints short.
11. NEVER call pm4py.read_xes, pm4py.read_ocel2_sqlite, pd.read_csv on the source log, or any other log-loading API inside python_code.
12. NEVER access the source dataset by file path. The already-loaded log is ONLY available through ACTIVE_LOG.
13. Prefer PM4Py high-level save helpers:
   - general DFG: pm4py.save_vis_dfg(...)
   - general Petri net: pm4py.save_vis_petri_net(...)
   - ocel OCDFG: pm4py.save_vis_ocdfg(...)
   - ocel OCPN: pm4py.save_vis_ocpn(...)
14. Do not use low-level visualizer.apply(..., output_path=...) patterns unless absolutely necessary.
15. Important OCEL return-shape facts:
   - pm4py.discover_ocdfg(ocel) returns ONE dict object, not multiple values.
   - For OCDFG node count, use len(ocdfg["activities"]). There is no ocdfg["nodes"] key.
   - For OCDFG edge tables, inspect ocdfg["edges"]["event_couples"] and derive counts from len(linked_pairs).
   - For OCDFG total edge count, use sum(len(edge_map) for edge_map in ocdfg["edges"]["event_couples"].values()).
   - pm4py.discover_oc_petri_net(ocel) returns ONE dict object.
   - OCPN nets are under ocpn["petri_nets"], where each value is a tuple: (net, initial_marking, final_marking).
16. If you create a flattened OCEL view, call pm4py.ocel_flattening with the real PM4Py signature: pm4py.ocel_flattening(ocel, "orders") or pm4py.ocel_flattening(ocel, object_type="orders"). Do not invent keyword names like exact_object_type.
17. If you create a flattened OCEL view, downstream discovery must use case-centric APIs such as pm4py.discover_petri_net_inductive(flat_log), not pm4py.discover_oc_petri_net(flat_log).
18. Do not json.dump raw PM4Py objects such as ocdfg, ocpn, petri net tuples, markings, or sets. Serialize concise JSON-ready summaries only.
""".strip()

        return f"""
You are a single PM4Py coding agent.
Solve the user's process-mining task end-to-end in one shot.

[User Query]
{query}

[Task Metadata]
- category: {category}
- query_no: {query_no or 'N/A'}
- baseline_variant: {self.spec.label}

[Controlled Context]
{chr(10).join(context_blocks)}

{dataset_rules}

{reference_patterns}

{python_contract}

Return ONLY valid JSON.
""".strip()

    def validate_generated_code(self, python_code: str, category: str):
        forbidden = [
            "pm4py.read_xes(",
            "pm4py.read_ocel2_sqlite(",
            "pd.read_csv(",
            "pm4py.read_ocel(",
            "visualizer.apply(",
            self.cfg.input_xes_general,
            self.cfg.input_sqlite_ocel,
        ]
        hits = [pattern for pattern in forbidden if pattern in python_code]
        if hits:
            raise ValueError(f"Generated code illegally reloads source data: {hits}")
        if "def main" not in python_code:
            raise ValueError("Generated code must define main().")
        if "json.dumps(" not in python_code:
            raise ValueError("Generated code must print the final answer with json.dumps(...).")
        if "ocdfg['nodes']" in python_code or 'ocdfg["nodes"]' in python_code:
            raise ValueError('Generated code incorrectly assumes OCDFG has a "nodes" key; use len(ocdfg["activities"]).')
        if "json.dump(ocdfg" in python_code or 'json.dump(ocpn' in python_code:
            raise ValueError("Generated code attempts to json.dump a raw PM4Py object; save a JSON-serializable summary instead.")
        expected_binding = "event_log = ACTIVE_LOG" if category == "general" else "ocel = ACTIVE_LOG"
        if expected_binding not in python_code:
            raise ValueError(f"Generated code must bind the active log with `{expected_binding}`.")
        if category == "general":
            invalid_hits = [
                pattern for pattern in ["ocel.events", "ocel.objects", "ocel.relations", "pm4py.ocel_flattening("]
                if pattern in python_code
            ]
            if invalid_hits:
                raise ValueError(f"General generated code uses OCEL-only patterns: {invalid_hits}")
        else:
            if "ocel['" in python_code or 'ocel["' in python_code:
                raise ValueError("OCEL code must use attribute access such as `ocel.events`, not dictionary subscripting.")
            if "exact_object_type=" in python_code:
                raise ValueError("Use the real PM4Py flattening signature: `pm4py.ocel_flattening(ocel, \"orders\")` or `object_type=\"orders\"`, not `exact_object_type=`.")
            invalid_helpers = [
                pattern for pattern in ["flatten_ocel_view", "pm4py.flatten_ocel(", "load_reference_ocpn", "load_reference_petri"]
                if pattern in python_code
            ]
            if invalid_helpers:
                raise ValueError(f"OCEL code uses unsupported helper names: {invalid_helpers}")
            if re.search(r"pm4py\.discover_oc_petri_net\(\s*[^)]*(flat|flatten)", python_code):
                raise ValueError("Flattened OCEL views must use case-centric discovery such as `pm4py.discover_petri_net_inductive(flat_log)`.")
            if "pm4py.save_vis_ocpn(" in python_code and "pm4py.ocel_flattening(" in python_code:
                raise ValueError("Flattened Petri nets must use `pm4py.save_vis_petri_net(...)`, not `pm4py.save_vis_ocpn(...)`.")
            if any(pattern in python_code for pattern in [
                'ocpn["places"]',
                "ocpn['places']",
                'ocpn["transitions"]',
                "ocpn['transitions']",
                'ocpn["arcs"]',
                "ocpn['arcs']",
            ]):
                raise ValueError("OCPN summaries must inspect `ocpn['petri_nets']` per object type, not top-level places/transitions/arcs.")

    def generate_solution(
        self,
        query_no: str,
        query: str,
        category: str,
        query_type: str,
        answer_format: str,
        interpretation: str,
        state: Dict[str, Any],
    ):
        prompt = self.build_prompt(query_no, query, category, query_type, answer_format, interpretation, state)
        response = self.llm.invoke([("system", prompt), ("human", query)])
        raw = getattr(response, "content", "")
        if not isinstance(raw, str):
            raw = str(raw)
        parsed = clean_and_parse_json(raw)
        analysis_plan = str(parsed.get("analysis_plan", "")).strip()
        python_code = str(parsed.get("python_code", "")).strip()
        if not analysis_plan or not python_code:
            raise ValueError("LLM output is missing analysis_plan or python_code.")
        self.validate_generated_code(python_code, category)
        return analysis_plan, python_code

    def execute_code(self, python_code: str, category: str):
        active_log = self.load_xes_once() if category == "general" else self.load_ocel_once()
        buffer = io.StringIO()
        execution_error = None
        try:
            exec_globals = {
                "pm4py": pm4py,
                "pd": pd,
                "np": np,
                "json": json,
                "os": os,
                "pickle": pickle,
                "math": math,
                "statistics": statistics,
                "collections": collections,
                "sqlite3": sqlite3,
                "ACTIVE_LOG": active_log,
            }
            exec(python_code, exec_globals)
            if "main" not in exec_globals:
                raise ValueError("No main() function found in generated code.")
            with redirect_stdout(buffer):
                exec_globals["main"]()
        except Exception:
            execution_error = traceback.format_exc()

        stdout = buffer.getvalue().strip()
        if execution_error:
            raise RuntimeError(execution_error)
        if not stdout:
            raise RuntimeError("No output captured from execution.")

        non_empty_lines = [line.strip() for line in stdout.splitlines() if line.strip()]
        if not non_empty_lines:
            raise RuntimeError("No non-empty output lines captured from execution.")
        final_line = non_empty_lines[-1]
        try:
            result_payload = json.loads(final_line)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "Final line is not valid JSON. "
                f"Expected json.dumps(final_answer, ensure_ascii=False) as the last line.\n{stdout}"
            ) from exc

        return json_ready(result_payload), stdout

    def run_single_query(
        self,
        query_no: Any,
        query: str,
        category: str,
        query_type: str,
        answer_format: str,
        interpretation: str,
    ) -> Dict[str, Any]:
        state: Dict[str, Any] = {
            "analysis_plan": "",
            "python_code": "",
            "error": None,
            "attempt_count": 0,
            "stdout": "",
        }

        for attempt in range(MAX_RETRIES + 1):
            state["attempt_count"] = attempt
            LogUtils.header(f"{self.spec.label} | query={query_no} | attempt={attempt + 1}")
            LogUtils.info("Category", category)
            LogUtils.info("Query", query, truncate_len=700)

            try:
                analysis_plan, python_code = self.generate_solution(
                    query_no=str(query_no),
                    query=query,
                    category=category,
                    query_type=query_type,
                    answer_format=answer_format,
                    interpretation=interpretation,
                    state=state,
                )
                state["analysis_plan"] = analysis_plan
                state["python_code"] = python_code
                LogUtils.info("Analysis Plan", analysis_plan, truncate_len=3000)
                LogUtils.code_snippet("Generated Code Preview", "\n".join(python_code.splitlines()[:40]))

                result_payload, stdout = self.execute_code(python_code, category)
                state["stdout"] = stdout
                return {
                    "status": "success",
                    "analysis_plan": analysis_plan,
                    "python_code": python_code,
                    "result": result_payload,
                    "stdout": stdout,
                    "attempt_count": attempt + 1,
                }
            except Exception as exc:
                state["error"] = str(exc)
                LogUtils.error("Attempt Failed", details=state["error"])
                if attempt >= MAX_RETRIES:
                    return {
                        "status": "error",
                        "analysis_plan": state.get("analysis_plan", ""),
                        "python_code": state.get("python_code", ""),
                        "error": state["error"],
                        "stdout": state.get("stdout", ""),
                        "attempt_count": attempt + 1,
                    }

        return {
            "status": "error",
            "analysis_plan": state.get("analysis_plan", ""),
            "python_code": state.get("python_code", ""),
            "error": state.get("error", "Unknown error"),
            "stdout": state.get("stdout", ""),
            "attempt_count": state.get("attempt_count", 0),
        }

    def save_query_artifacts(self, query_no: Any, final_state: Dict[str, Any]):
        save_json_file(self.cfg.get_analysis_plan_path(query_no), {"analysis_plan": final_state.get("analysis_plan", "")})
        code_path = self.cfg.get_code_path(query_no)
        with open(code_path, "w", encoding="utf-8") as f:
            f.write(final_state.get("python_code", ""))
        save_json_file(self.cfg.get_result_json_path(query_no), final_state)

    def run(self, target_nos: Optional[List[str]] = None):
        query_path = self.cfg.query_file
        if not os.path.exists(query_path):
            raise FileNotFoundError(f"Query file not found: {query_path}")

        df = self.load_query_file(query_path)
        if target_nos:
            if "no" not in df.columns:
                raise ValueError("'no' column is required when using RUN_QUERY_NOS.")
            wanted = {str(x) for x in target_nos}
            df = df[df["no"].astype(str).isin(wanted)]

        if self.spec.result_col not in df.columns:
            df[self.spec.result_col] = ""

        output_path = self.cfg.get_output_csv_path()

        for index, row in df.iterrows():
            query = get_query_text(row)
            if not query:
                continue
            category = get_query_category(row)
            query_type = get_query_type(row)
            answer_format = get_answer_format(row)
            interpretation = get_query_interpretation(row)
            query_no = get_query_no(row, default=index + 1)

            final_state = self.run_single_query(
                query_no=query_no,
                query=query,
                category=category,
                query_type=query_type,
                answer_format=answer_format,
                interpretation=interpretation,
            )
            self.save_query_artifacts(query_no, final_state)
            final_json = json.dumps(json_ready(final_state), ensure_ascii=False)
            df.at[index, self.spec.result_col] = final_json
            df.to_csv(output_path, index=False, encoding="utf-8-sig")
            print(f"   {LogUtils.GREEN}✅ Progress Saved (Query {index + 1}/{len(df)}){LogUtils.RESET}")

        print(f"\n{LogUtils.GREEN}All Completed. Final results saved to: {output_path}{LogUtils.RESET}")


def run_baseline(script_path: str, spec: BaselineSpec):
    runner = SingleAgentBaselineRunner(script_path=script_path, spec=spec)
    sys.stdout = DualLogger(runner.cfg.log_file)
    sys.stderr = sys.stdout
    runner.run(target_nos=parse_target_nos_env() or None)
