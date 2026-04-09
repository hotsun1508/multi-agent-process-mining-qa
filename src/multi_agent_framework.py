import os
import sys
import io
import json
import traceback
import copy
import pickle
import math
import statistics
import collections
import pandas as pd
import numpy as np
import pm4py
import re
import warnings
import sqlite3 # [Added] Required for generated execution code
import matplotlib
import matplotlib.pyplot as plt # [Added] Required for visualization code

from pathlib import Path
from dotenv import load_dotenv
from contextlib import redirect_stdout
from typing import TypedDict, Annotated, List, Dict, Any, Union, Tuple
from datetime import datetime

# LangChain / LangGraph Imports
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.documents import Document
from langgraph.graph import StateGraph, END

# [Fix] Silence noisy warning messages
warnings.filterwarnings("ignore", category=UserWarning)
# [Fix] Prevent GUI windows in server environments
matplotlib.use('Agg')

# --------------------------------------------------
# 0. Environment & Logging Setup
# --------------------------------------------------
SRC_DIR = Path(__file__).resolve().parent
REPO_ROOT = SRC_DIR.parent
load_dotenv(REPO_ROOT / ".env")

if not os.getenv("OPENAI_API_KEY"):
    raise ValueError("OPENAI_API_KEY is not set in the .env file.")

# Model configuration
# --------------------------------------------------
# [Cost Optimization Strategy]
# --------------------------------------------------
# 1. Debugging Mode (All mini): useful for experiment loops at minimal cost.
llm_supervisor = ChatOpenAI(model="gpt-5.4-mini", temperature=0)
llm_worker = ChatOpenAI(model="gpt-5.4-mini", temperature=0)

# 2. Hybrid Mode (Recommended): cheaper Supervisor/Assembler, stronger Coder.
# llm_supervisor = ChatOpenAI(model="gpt-4o-mini", temperature=0) # mini is usually enough for planning
# llm_worker = ChatOpenAI(model="gpt-5.4", temperature=0)      # gpt-5.4 is more reliable for code generation

# 3. Final Benchmark Mode: enable only for final paper-ready benchmark runs.
# llm_supervisor = ChatOpenAI(model="gpt-5.4", temperature=0)
# llm_worker = ChatOpenAI(model="gpt-5.4", temperature=0)

class LogUtils:
    """Logging utilities optimized for debugging."""
    GREEN = "\033[92m"
    CYAN = "\033[96m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    RESET = "\033[0m"
    BOLD = "\033[1m"

    @staticmethod
    def header(title):
        print(f"\n{LogUtils.CYAN}{'='*60}\n[ {title} ]\n{'='*60}{LogUtils.RESET}")

    @staticmethod
    def node_start(node_name, retry=0):
        retry_msg = f"(Retry: {retry})" if retry > 0 else ""
        print(f"\n{LogUtils.BOLD}{LogUtils.GREEN}▶ [{node_name} Node] Started {retry_msg}{LogUtils.RESET}")

    @staticmethod
    def info(key, value, truncate_len=300):
        str_val = str(value)
        if len(str_val) > truncate_len:
            display_val = str_val[:truncate_len] + f"... (total {len(str_val)} chars)"
        else:
            display_val = str_val
        print(f"   - {LogUtils.BOLD}{key}:{LogUtils.RESET} {display_val}")

    @staticmethod
    def code_snippet(title, code):
        lines = code.strip().split('\n')
        if len(lines) > 10:
            preview = "\n".join(lines[:5]) + "\n   ... (omitted) ...\n" + "\n".join(lines[-3:])
        else:
            preview = code
        print(f"   - {LogUtils.BOLD}{title}:{LogUtils.RESET}\n{LogUtils.YELLOW}{preview}{LogUtils.RESET}")

    @staticmethod
    def error(msg, details=""):
        print(f"\n{LogUtils.RED}✖ [ERROR]{LogUtils.RESET} {msg}")
        if details:
            print(f"{details}") 

    @staticmethod
    def print_code(code):
        lines = code.split('\n')
        print(f"{LogUtils.YELLOW}--- FAILED CODE DUMP ---{LogUtils.RESET}")
        for i, line in enumerate(lines):
            print(f"{i+1:03d} | {line}")
        print(f"{LogUtils.YELLOW}------------------------{LogUtils.RESET}")
    
    # [Added] Log retrieved RAG sources
    @staticmethod
    def print_rag_sources(docs, title="RAG Retrieval Results"):
        print(f"\n   {LogUtils.CYAN}[{title}]{LogUtils.RESET}")
        if not docs:
            print("     (No documents found)")
            return

        for i, doc in enumerate(docs):
            # The item may be either a Document or a (Document, score) tuple.
            if isinstance(doc, tuple):
                content = doc[0].page_content
                meta = doc[0].metadata
                score = doc[1]
                score_str = f"(Score: {score:.4f})"
            else:
                content = doc.page_content
                meta = doc.metadata
                score_str = ""

            func_name = meta.get('function_name', 'Manual Chunk')
            # Content preview with line breaks removed
            # preview = content.replace('\n', ' ').strip()[:100]
            preview = content.replace('\n', ' ').strip()[:300]
            
            print(f"     {i+1}. {LogUtils.BOLD}[{func_name}]{LogUtils.RESET} {score_str}")
            print(f"        {LogUtils.YELLOW}\"{preview}...\"{LogUtils.RESET}")

# [Added] JSON parsing helper
def clean_and_parse_json(content: str):
    """Extract pure JSON even if the LLM output includes Markdown or extra text."""
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        content = content.replace("```json", "").replace("```", "").strip()
        match = re.search(r'(\{.*\})', content, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass
        raise ValueError(f"JSON Parsing Failed. Raw content: {content[:100]}...")

print(f"[Init] LLM initialized. Supervisor & Worker: gpt-5.4-mini")


# --------------------------------------------------
# 1. Configuration
# --------------------------------------------------
class ProjectConfig:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    CURRENT_SCRIPT_NAME = os.path.basename(__file__)
    script_name = CURRENT_SCRIPT_NAME.replace('.py', '')
    FILE_PREFIX = f"{timestamp}_{script_name}"

    ARTIFACTS_DIR = str(REPO_ROOT / "artifacts" / FILE_PREFIX)
    BASE_DIR = ARTIFACTS_DIR
    DATA_DIR = str(REPO_ROOT / "data")
    INPUT_SQLITE_OCEL = os.getenv(
        "INPUT_SQLITE_OCEL",
        str(REPO_ROOT / "data/order-management.sqlite"),
    )
    INPUT_XES_GENERAL = os.getenv(
        "INPUT_XES_GENERAL",
        str(REPO_ROOT / "data/BPI Challenge 2017.xes"),
    )
    
    RAG_DB_PATH = str(REPO_ROOT / "resources" / "pm4py_faiss_db")
    MANUAL_TXT_PATH = str(REPO_ROOT / "resources" / "pm4py_manual.txt")
    OUTPUT_REL_DIR = "output"
    OUTPUT_IMAGE_DIR = os.path.join(ARTIFACTS_DIR, OUTPUT_REL_DIR)
    EVAL_DIR = ARTIFACTS_DIR
    QUERY_FILE = os.getenv(
        "QUERY_FILE",
        str(REPO_ROOT / "query/benchmark_120_complex_pm_queries.xlsx"),
    )
    GENERAL_SCHEMA_FILE = str(REPO_ROOT / "data/BPI Challenge 2017_data_schema.json")
    OCEL_SCHEMA_FILE = str(REPO_ROOT / "data/order-management_data_schema.json")

    CONTEXT_DIR = str(REPO_ROOT / "context")
    SCHEMA_FILE = os.path.join(CONTEXT_DIR, "data_schema.json")
    # Dataset-specific abstraction files are precomputed and stored under context/.
    GENERAL_ABSTRACTION_FILE = os.path.join(CONTEXT_DIR, "bpi-challenge-2017_pm_abstractions.json")
    OCEL_ABSTRACTION_FILE = os.path.join(CONTEXT_DIR, "order-management_pm_abstractions.json")
    
    # [Mod] Log path and filename configuration (timestamp included)
    LOG_DIR = ARTIFACTS_DIR
    LOG_FILE = os.path.join(LOG_DIR, f"{FILE_PREFIX}_log.txt")

    @staticmethod
    def get_output_csv_path():
        filename = f"{ProjectConfig.FILE_PREFIX}.csv"
        return os.path.join(ProjectConfig.EVAL_DIR, filename)

for directory in [
    ProjectConfig.ARTIFACTS_DIR,
    ProjectConfig.BASE_DIR,
    ProjectConfig.OUTPUT_IMAGE_DIR,
    ProjectConfig.EVAL_DIR,
    ProjectConfig.LOG_DIR,
]:
    os.makedirs(directory, exist_ok=True)

try:
    os.chdir(ProjectConfig.BASE_DIR)
except Exception:
    pass

# --------------------------------------------------
# [New Class] DualLogger: write to terminal and file simultaneously
# --------------------------------------------------
class DualLogger:
    def __init__(self, filepath):
        self.terminal = sys.stdout
        self.log = open(filepath, "w", encoding='utf-8') # 'w' overwrites; 'a' appends
        self.ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

    def write(self, message):
        # 1. Terminal output (keep colors)
        self.terminal.write(message)
        
        # 2. File output (strip ANSI colors)
        clean_message = self.ansi_escape.sub('', message)
        self.log.write(clean_message)
        self.log.flush() # flush immediately

    def flush(self):
        self.terminal.flush()
        self.log.flush()

# --------------------------------------------------
# 2. RAG Manager
# --------------------------------------------------
class RAGManager:
    def __init__(self, db_path):
        self.embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        self.db_path = db_path
        
        # Try to load an existing DB
        if os.path.exists(db_path):
            try:
                self.vector_db = FAISS.load_local(db_path, self.embeddings, allow_dangerous_deserialization=True)
                print(f"[RAG] ✅ Loaded existing DB from {db_path}")
            except Exception as e:
                print(f"[RAG] ⚠️ Error loading DB: {e}. Re-initializing...")
                self._init_new_db()
        else:
            print("[RAG] ⚠️ DB not found. Initializing new DB...")
            self._init_new_db()

    def _init_new_db(self):
        """Initialize the DB and load the manual using smart parsing."""
        # Create an empty starter DB
        self.vector_db = FAISS.from_texts(["PM4Py RAG Initialization"], self.embeddings)
        
        # Parse and add the manual if available
        if os.path.exists(ProjectConfig.MANUAL_TXT_PATH):
            self.load_and_chunk_manual(ProjectConfig.MANUAL_TXT_PATH)

    def _get_category(self, text):
        """Infer a coarse category from text."""
        text_lower = text.lower()
        if "discover" in text_lower: return "discovery"
        if "conformance" in text_lower: return "conformance"
        if "filter" in text_lower: return "filtering"
        if "view" in text_lower or "save_vis" in text_lower: return "visualization"
        return "utility"

    def load_and_chunk_manual(self, file_path):
        """
        [Smart Parsing] Read the manual and split it into 'tool' (functions)
        and 'concept' (descriptive sections).
        This follows the same logic as build_rag.py.
        """
        print(f"[RAG] 📖 Loading and Smart Parsing Manual from {file_path}...")
        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()
            
        func_pattern = re.compile(r"^(pm4py\.[a-zA-Z0-9_\.]+)\((.*?)\)(.*)$")
        lines = text.split('\n')
        
        documents = []
        current_content = []
        # Start by assuming the content begins with a descriptive concept section.
        current_meta = {"type": "concept", "source": "manual", "category": "general", "is_object_centric": False}

        for line in lines:
            line = line.strip()
            if not line: continue

            match = func_pattern.match(line)
            
            # [CASE 1] Function signature found -> save as a 'tool'
            if match:
                # Save the previous buffered content
                if current_content:
                    full_text = "\n".join(current_content)
                    if len(full_text) > 20:
                        documents.append(Document(page_content=full_text, metadata=current_meta))
                
                # Start a new 'tool' block
                func_name = match.group(1)
                args_str = match.group(2)
                full_line = line
                
                meta = {
                    "type": "tool",       # Key idea: function definitions are stored as tools
                    "source": "manual",
                    "function_name": func_name,
                    "category": self._get_category(func_name),
                    "is_object_centric": ("ocel" in func_name.lower() or "ocel" in args_str.lower()),
                    "input_type": "OCEL" if ("ocel" in func_name.lower() or "ocel" in args_str.lower()) else "DataFrame",
                    "return_type_hint": "tuple" if "Tuple" in full_line else ("dict" if "Dict" in full_line else "simple")
                }
                
                current_meta = meta
                current_content = [f"[Function Signature] {line}"]
            
            # [CASE 2] Uppercase header found -> start a new 'concept' section
            elif line.isupper() and len(line) > 3 and "PM4PY" not in line:
                if current_content:
                    full_text = "\n".join(current_content)
                    documents.append(Document(page_content=full_text, metadata=current_meta))
                
                current_meta = {
                    "type": "concept",    # Key idea: descriptive text is stored as concepts
                    "source": "manual",
                    "category": self._get_category(line),
                    "section_title": line,
                    "is_object_centric": ("OBJECT" in line and "CENTRIC" in line)
                }
                current_content = [line]

            # [CASE 3] Plain text -> append to the current section
            else:
                current_content.append(line)

        # Save the final buffered section
        if current_content:
            documents.append(Document(page_content="\n".join(current_content), metadata=current_meta))
            
        if documents:
            self.vector_db.add_documents(documents)
            self.vector_db.save_local(self.db_path)
            print(f"[RAG] ✅ {len(documents)} manual chunks parsed & saved.")

    def search_context(self, query: str, k=5, filter_type=None):
        """
        filter_type: 
          - None: search all content (for Supervisor)
          - "tool": search only functions/code (for Tool Generator)
          - "concept": search only explanatory sections
        """
        filter_dict = {"type": filter_type} if filter_type else None
        
        try:
            results = self.vector_db.similarity_search_with_score(
                query, k=k, filter=filter_dict
            )
            return results
        except Exception as e:
            print(f"[RAG] ❌ Search Error: {e}")
            return []

    def check_tool_cache(self, tool_plan: str, threshold=0.2): 
        """Search cached generated tools first."""
        try:
            # Search using the 'source=generated' filter
            results = self.vector_db.similarity_search_with_score(
                tool_plan, k=1, filter={"type": "tool", "source": "generated"}
            )
            if results:
                doc, score = results[0]
                if score <= threshold:
                    print(f"   [RAG Cache Hit] 🚀 Found cached tool (Dist: {score:.4f})")
                    return doc.page_content
            return None
        except Exception:
            return None

    def save_new_tool(self, tool_plan: str, tool_code: str):
        """Persist successful generated code with source='generated'."""
        if self.check_tool_cache(tool_plan, threshold=0.1):
            return

        doc = Document(
            page_content=tool_code,
            metadata={
                "type": "tool", 
                "source": "generated",
                "plan": tool_plan, 
                "timestamp": str(datetime.now())
            }
        )
        self.vector_db.add_documents([doc])
        self.vector_db.save_local(self.db_path)
        print(f"   {LogUtils.CYAN}[RAG] ✨ New generated tool saved to DB!{LogUtils.RESET}")

rag_manager = RAGManager(ProjectConfig.RAG_DB_PATH)

# --------------------------------------------------
# 3. LangGraph Nodes
# --------------------------------------------------
class AgentState(TypedDict):
    query_no: str
    query: str
    category: str
    benchmark_query_type: str
    answer_format: str
    query_interpretation: str
    query_requirements: str
    data_summary: str      # original concise summary
    rich_context: str      # [New] detailed schema + process context
    tool_plan: str
    analysis_plan: str
    tool_code_list: List[str]
    last_generated_tool_code: str
    last_failed_code: str # store failed code so Supervisor can get feedback
    final_code: str
    execution_result: str
    error: Union[str, None]
    retry_count: int


_DATASET_CONTEXT_CACHE: Dict[str, str] = {}
_DATASET_SCHEMA_CACHE: Dict[str, Dict[str, Any]] = {}
_RUNTIME_DATA_CACHE: Dict[str, Any] = {}
_RUNTIME_PROFILE_CACHE: Dict[str, str] = {}
_GENERAL_ABSTRACTION_CONTEXT_CACHE: Union[str, None] = None
_OCEL_ABSTRACTION_CONTEXT_CACHE: Union[str, None] = None
_OCEL_FLATTENED_CONTEXT_CACHE: Union[str, None] = None


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
    pairs = list(values.items())[:limit]
    rendered = [f"{k}={v}" for k, v in pairs]
    suffix = " ..." if len(values) > limit else ""
    return ", ".join(rendered) + suffix


def get_schema_file(category: str) -> str:
    return ProjectConfig.GENERAL_SCHEMA_FILE if category == "general" else ProjectConfig.OCEL_SCHEMA_FILE


def get_dataset_schema(category: str) -> Dict[str, Any]:
    if category not in _DATASET_SCHEMA_CACHE:
        _DATASET_SCHEMA_CACHE[category] = load_json_file_safe(get_schema_file(category))
    return _DATASET_SCHEMA_CACHE[category]


def get_runtime_input(category: str):
    if category not in _RUNTIME_DATA_CACHE:
        if category == "general":
            _RUNTIME_DATA_CACHE[category] = pm4py.read_xes(ProjectConfig.INPUT_XES_GENERAL)
        else:
            _RUNTIME_DATA_CACHE[category] = pm4py.read_ocel2_sqlite(ProjectConfig.INPUT_SQLITE_OCEL)
    return _RUNTIME_DATA_CACHE[category]


def build_runtime_profile(category: str) -> str:
    if category in _RUNTIME_PROFILE_CACHE:
        return _RUNTIME_PROFILE_CACHE[category]

    try:
        if category == "general":
            event_log = get_runtime_input("general")
            log_df = pm4py.convert_to_dataframe(event_log)
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
            ocel = get_runtime_input("ocel")
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

    _RUNTIME_PROFILE_CACHE[category] = runtime_profile
    return runtime_profile


def clip_text(text: Any, limit: int = 900) -> str:
    value = str(text or "").strip()
    if not value:
        return "N/A"
    value = re.sub(r"\s+", " ", value)
    if len(value) <= limit:
        return value
    return value[:limit] + f"... (truncated, total {len(value)} chars)"


def build_general_abstraction_context() -> str:
    global _GENERAL_ABSTRACTION_CONTEXT_CACHE
    if _GENERAL_ABSTRACTION_CONTEXT_CACHE is not None:
        return _GENERAL_ABSTRACTION_CONTEXT_CACHE

    abstraction = load_json_file_safe(ProjectConfig.GENERAL_ABSTRACTION_FILE)
    if not abstraction:
        _GENERAL_ABSTRACTION_CONTEXT_CACHE = (
            "[General Abstraction Context]\n"
            f"- Not available at: {ProjectConfig.GENERAL_ABSTRACTION_FILE}"
        )
        return _GENERAL_ABSTRACTION_CONTEXT_CACHE

    metrics = abstraction.get("summary_metrics", {})
    _GENERAL_ABSTRACTION_CONTEXT_CACHE = f"""
[General Abstraction Context]
- Source abstraction file: {ProjectConfig.GENERAL_ABSTRACTION_FILE}
- Source dataset: {abstraction.get('source_dataset', ProjectConfig.INPUT_XES_GENERAL)}
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
    return _GENERAL_ABSTRACTION_CONTEXT_CACHE


def build_ocel_abstraction_context() -> str:
    global _OCEL_ABSTRACTION_CONTEXT_CACHE
    if _OCEL_ABSTRACTION_CONTEXT_CACHE is not None:
        return _OCEL_ABSTRACTION_CONTEXT_CACHE

    abstraction = load_json_file_safe(ProjectConfig.OCEL_ABSTRACTION_FILE)
    if not abstraction:
        _OCEL_ABSTRACTION_CONTEXT_CACHE = (
            "[OCEL Abstraction Context]\n"
            f"- Not available at: {ProjectConfig.OCEL_ABSTRACTION_FILE}"
        )
        return _OCEL_ABSTRACTION_CONTEXT_CACHE

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
    _OCEL_ABSTRACTION_CONTEXT_CACHE = f"""
[OCEL Abstraction Context]
- Source abstraction file: {ProjectConfig.OCEL_ABSTRACTION_FILE}
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
    return _OCEL_ABSTRACTION_CONTEXT_CACHE


def build_ocel_flattened_context() -> str:
    global _OCEL_FLATTENED_CONTEXT_CACHE
    if _OCEL_FLATTENED_CONTEXT_CACHE is not None:
        return _OCEL_FLATTENED_CONTEXT_CACHE

    try:
        ocel = get_runtime_input("ocel")
        lines = ["[Flattened OCEL Runtime Examples]"]
        for object_type in ["orders", "customers", "packages"]:
            flat_log = pm4py.ocel_flattening(ocel, object_type)
            preview_cols = ", ".join([str(col) for col in list(flat_log.columns)[:8]])
            lines.append(
                f"- {object_type}: rows={len(flat_log)}, columns={preview_cols}"
            )
        lines.append("- Flattened logs are case-centric dataframes.")
        lines.append("- Use `case:concept:name` as the case id column, `concept:name` as the activity column, and `time:timestamp` as the timestamp column.")
        lines.append("- Do not expect `case_id` or `ocel:activity` inside flattened logs.")
        _OCEL_FLATTENED_CONTEXT_CACHE = "\n".join(lines)
    except Exception as exc:
        _OCEL_FLATTENED_CONTEXT_CACHE = (
            "[Flattened OCEL Runtime Examples]\n"
            f"- Unavailable: {exc}"
        )

    return _OCEL_FLATTENED_CONTEXT_CACHE


def get_query_category(row: pd.Series) -> str:
    raw = str(row.get("category", "ocel")).strip().lower()
    return "general" if raw == "general" else "ocel"


def get_query_no(row: pd.Series) -> str:
    for col in ["no", "No", "count no", "Count No"]:
        if col in row and pd.notna(row.get(col)):
            return str(row.get(col)).strip()
    return ""


def get_query_type(row: pd.Series) -> str:
    return str(row.get("query type", row.get("Query Type", "Unknown"))).strip()


def get_answer_format(row: pd.Series) -> str:
    value = row.get("Answer format", row.get("answer format", ""))
    if pd.isna(value):
        return ""
    return str(value).strip()


def get_query_interpretation(row: pd.Series) -> str:
    for col in ["해석", "interpretation", "Interpretation"]:
        if col in row and pd.notna(row.get(col)):
            return str(row.get(col)).strip()
    return ""


def parse_answer_format(answer_format: str) -> Union[Dict[str, Any], None]:
    if not answer_format:
        return None
    try:
        parsed = json.loads(answer_format)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def normalize_free_text(text: Any) -> str:
    lowered = str(text).strip().lower()
    lowered = re.sub(r"[_/()\-]+", " ", lowered)
    lowered = re.sub(r"[^a-z0-9\s]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def normalize_bpi_activity_label(label: str) -> str:
    normalized = normalize_free_text(label)
    return re.sub(r"^[a-z]\s+", "", normalized).strip()


def resolve_general_activity_aliases(query: str, interpretation: str, schema: Dict[str, Any]) -> List[Dict[str, str]]:
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
        aliases.append({
            "query_phrase": phrase,
            "dataset_activity": exact_activity,
        })
    return aliases[:8]


def build_query_requirements(query_no: str, category: str, query: str, interpretation: str) -> str:
    schema = get_dataset_schema(category)
    lower = f"{query} {interpretation}".lower()
    lines = ["[Query-Specific Requirements]"]
    lines.append(f"- Query no: {query_no or 'N/A'}")
    if interpretation:
        lines.append(f"- Interpretation hint: {interpretation}")

    if category == "general":
        aliases = resolve_general_activity_aliases(query, interpretation, schema)
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
        object_types = [str(x) for x in schema.get("object_types", [])]
        mentioned_object_types = [obj for obj in object_types if obj in lower]
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
        for obj in object_types:
            patterns = [
                f"flattened {obj} view",
                f"using {obj} as the case notion",
                f"each {obj} object is treated as a case",
                f"full flattened {obj} view",
            ]
            if any(pattern in lower for pattern in patterns):
                flatten_object_type = obj
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


def get_data_context(category: str) -> str:
    if category == "general":
        return (
            "[Dataset Summary]\n"
            f"- Category: general (case-centric event log)\n"
            f"- Dataset: BPI Challenge 2017\n"
            f"- Format: XES\n"
            f"- Path: {ProjectConfig.INPUT_XES_GENERAL}"
        )
    return (
        "[Dataset Summary]\n"
        f"- Category: ocel (object-centric event log)\n"
        f"- Dataset: order-management\n"
        f"- Format: OCEL 2.0 (SQLite)\n"
        f"- Path: {ProjectConfig.INPUT_SQLITE_OCEL}"
    )


def build_dataset_context(category: str, query_requirements: str = "") -> str:
    if category not in _DATASET_CONTEXT_CACHE:
        schema = get_dataset_schema(category)
        runtime_profile = build_runtime_profile(category)

        if category == "general":
            context = f"""
[Dataset Context: BPI Challenge 2017]
- Execution input: {ProjectConfig.INPUT_XES_GENERAL}
- Schema source: {ProjectConfig.GENERAL_SCHEMA_FILE}
- General abstraction source: {ProjectConfig.GENERAL_ABSTRACTION_FILE}
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

{build_general_abstraction_context()}

{runtime_profile}
""".strip()
        else:
            context = f"""
[Dataset Context: order-management OCEL]
- Execution input: {ProjectConfig.INPUT_SQLITE_OCEL}
- Semantic schema source: {ProjectConfig.OCEL_SCHEMA_FILE}
- OCEL abstraction source: {ProjectConfig.OCEL_ABSTRACTION_FILE}
- Format: {schema.get('format', 'OCEL 2.0')}
- PM4Py event columns: ocel:eid, ocel:timestamp, ocel:activity
- PM4Py object columns: ocel:oid, role, weight, price, ocel:type
- PM4Py relation columns: ocel:eid, ocel:oid, ocel:qualifier, ocel:activity, ocel:timestamp, ocel:type
- Object types present in the dataset: {preview_list(schema.get('object_types'))}
- Activity names present in the dataset: {preview_list(schema.get('activities'), limit=30)}
- Object attributes present in the dataset: {preview_list(schema.get('object_attributes'))}
- Event attributes described by the semantic schema: {preview_list(schema.get('event_attributes'))}
- Important naming rule: the real order-management activities are lowercase strings with spaces (for example 'place order', 'pick item', 'send package').
- Never invent object types or flattened views that are not present in this dataset context.
- Allowed flattened primary object types: {preview_list(schema.get('object_types'))}
- For flattened views, use pm4py.ocel_flattening(ocel_or_restricted_ocel, object_type) where object_type is the exact dataset string such as "orders" or "packages", and then treat the result as a case-centric event log with columns like case:concept:name, concept:name, time:timestamp.
- Use OCEL accessors only: ocel.events, ocel.objects, ocel.relations.

{build_ocel_abstraction_context()}

{build_ocel_flattened_context()}
 
{runtime_profile}
""".strip()

        _DATASET_CONTEXT_CACHE[category] = context

    base_context = _DATASET_CONTEXT_CACHE[category]
    if query_requirements:
        return f"{base_context}\n\n{query_requirements}"
    return base_context


def build_benchmark_context(query_type: str, answer_format: str, interpretation: str = "") -> str:
    lines = [f"- Benchmark query type: {query_type or 'Unknown'}"]
    parsed = parse_answer_format(answer_format)
    if interpretation:
        lines.append(f"- Query interpretation hint: {interpretation}")
    if parsed:
        lines.append(f"- Expected result type: {parsed.get('result_type', 'N/A')}")
        lines.append(f"- Expected view: {parsed.get('view', 'N/A')}")
        result_schema = parsed.get("result_schema")
        if isinstance(result_schema, dict) and result_schema:
            lines.append(f"- Expected result schema keys: {', '.join(result_schema.keys())}")
    elif answer_format:
        lines.append(f"- Expected answer format: {answer_format}")
    return "[Benchmark Metadata]\n" + "\n".join(lines)


def validate_generated_tool_code(code: str, category: str) -> None:
    forbidden_common = [
        "pm4py.read_xes(",
        "pm4py.read_ocel2_sqlite(",
        ProjectConfig.INPUT_XES_GENERAL,
        ProjectConfig.INPUT_SQLITE_OCEL,
    ]
    common_hits = [pattern for pattern in forbidden_common if pattern in code]
    if common_hits:
        raise ValueError(f"Generated tool illegally reloads source data: {common_hits}")

    if "def " not in code:
        raise ValueError("Generated tool must define at least one function.")

    if category == "general":
        invalid_hits = [
            pattern for pattern in ["ocel.events", "ocel.objects", "ocel.relations", "pm4py.ocel_flattening("]
            if pattern in code
        ]
        if invalid_hits:
            raise ValueError(f"General tool uses OCEL-only patterns: {invalid_hits}")
        if "event_log" not in code:
            raise ValueError("General tool should use `event_log` as its primary input.")
    else:
        if "ocel['" in code or 'ocel["' in code:
            raise ValueError("OCEL tools must use attribute access such as `ocel.events`, not dictionary subscripting.")
        if re.search(r"\bocel\.(get_object_ids|get_event_ids_by_object_ids|filter_events|filter_objects)\s*\(", code):
            raise ValueError(
                "PM4Py OCEL objects in this environment do not expose helper methods like "
                "`get_object_ids`, `get_event_ids_by_object_ids`, or `filter_events`; use "
                "`ocel.events` / `ocel.objects` / `ocel.relations` plus `pm4py.filter_ocel_events(...)` "
                "or an explicit restricted-OCEL reconstruction."
            )
        if any(pattern in code for pattern in ["pm4py.ocel_filter(", "pm4py.ocel_propagate(", "pm4py.ocel_restrict("]):
            raise ValueError(
                "Do not call invented OCEL APIs like `pm4py.ocel_filter`, `pm4py.ocel_propagate`, or "
                "`pm4py.ocel_restrict`. Use `pm4py.filter_ocel_events(...)` or rebuild a restricted "
                "OCEL from filtered `events` / `objects` / `relations`."
            )
        if "exact_object_type=" in code:
            raise ValueError("Use the real PM4Py flattening signature: `pm4py.ocel_flattening(ocel, \"orders\")` or `object_type=\"orders\"`, not `exact_object_type=`.")
        invalid_helpers = [
            pattern for pattern in ["flatten_ocel_view", "pm4py.flatten_ocel(", "load_reference_ocpn", "load_reference_petri"]
            if pattern in code
        ]
        if invalid_helpers:
            raise ValueError(f"OCEL tool uses unsupported helper names: {invalid_helpers}")
        if re.search(r"(['\"](?:events|objects|relations)['\"]\s+(?:not\s+)?in\s+ocel|\battr\s+in\s+ocel\b|\bin\s+ocel\s+for\s+attr\b)", code):
            raise ValueError("OCEL objects are not iterable containers. Never check membership with `in ocel`; use attributes like `hasattr(ocel, 'events')` or direct `ocel.events` access.")
        if 'ocdfg["nodes"]' in code or "ocdfg['nodes']" in code:
            raise ValueError('OCDFG summaries must use `len(ocdfg["activities"])`, not a non-existent `nodes` key.')
        if "pm4py.ocel_flattening(" in code and "case_id" in code:
            raise ValueError("Flattened OCEL logs use `case:concept:name`, not `case_id`.")
        if "pm4py.ocel_flattening(" in code and "ocel:activity" in code:
            raise ValueError("Flattened OCEL logs use `concept:name`, not `ocel:activity`.")
        if re.search(r"['\"]events['\"]\s+not\s+in\s+ocel|['\"]relations['\"]\s+not\s+in\s+ocel", code):
            raise ValueError("OCEL objects are not iterable dictionaries. Use `hasattr(ocel, 'events')` / `hasattr(ocel, 'relations')` or direct attribute access.")
        if "event_couples" in code and "DataFrame(top_edges, columns=['Edge', 'Frequency'])" in code:
            raise ValueError("OCDFG edge tables must be normalized into rows like object_type/source/target/frequency before building a dataframe.")
        if any(pattern in code for pattern in [
            "ocpn.get(\"places\"",
            "ocpn.get('places'",
            "ocpn.get(\"transitions\"",
            "ocpn.get('transitions'",
            "ocpn.get(\"arcs\"",
            "ocpn.get('arcs'",
            "ocpn[\"places\"]",
            "ocpn['places']",
            "ocpn[\"transitions\"]",
            "ocpn['transitions']",
            "ocpn[\"arcs\"]",
            "ocpn['arcs']",
        ]):
            raise ValueError("OCPN summaries must read `ocpn['petri_nets']` per object type, not top-level places/transitions/arcs keys.")
        if re.search(r"pm4py\.discover_ocdfg\(\s*[^)]*(flat|flatten|sublog|model_building)", code):
            raise ValueError("Flattened or filtered case-centric logs must use `pm4py.discover_dfg(flat_log)`, not `pm4py.discover_ocdfg(...)`.")
        if "pm4py.save_vis_ocdfg(" in code and "pm4py.ocel_flattening(" in code:
            raise ValueError("Flattened case-centric DFGs must be visualized with `pm4py.save_vis_dfg(dfg, start_acts, end_acts, path)`, not `pm4py.save_vis_ocdfg(...)`.")
        if "pm4py.discover_dfg(" in code and any(
            pattern in code for pattern in ["dfg['edges']", 'dfg["edges"]', "dfg['activities']", 'dfg["activities"]', "dominant_variant", "average_case_duration"]
        ):
            raise ValueError("`pm4py.discover_dfg(flat_log)` returns `(dfg, start_acts, end_acts)` where `dfg` is a dict keyed by `(source, target)`, not an OCDFG-style dictionary.")
        if re.search(r"pm4py\.discover_oc_petri_net\(\s*[^)]*(flat|flatten|sublog|model_building)", code):
            raise ValueError("Flattened or filtered case-centric logs must use case-centric discovery such as `pm4py.discover_petri_net_inductive(flat_log)`, not `pm4py.discover_oc_petri_net(...)`.")
        if "pm4py.save_vis_ocpn(" in code and "pm4py.ocel_flattening(" in code:
            raise ValueError("Flattened case-centric Petri nets must be visualized with `pm4py.save_vis_petri_net(net, im, fm, path)`, not `pm4py.save_vis_ocpn(...)`.")
        if "pm4py.replay_log(" in code and "pm4py.discover_petri_net_inductive(" in code:
            raise ValueError("For flattened Petri-net conformance, use `pm4py.fitness_token_based_replay(...)` and `pm4py.conformance_diagnostics_token_based_replay(...)`, not `pm4py.replay_log(...)`.")
        if "json.dump(ocdfg" in code or "json.dump(ocpn" in code:
            raise ValueError("Serialize OCDFG/OCPN summaries, not the raw PM4Py objects.")
        if "print(json.dumps(result" in code and any(token in code for token in ['"ocpn"', '"petri_net"', '"net"', '"im"', '"fm"']):
            raise ValueError("Do not JSON-serialize result dictionaries that still contain raw PM4Py objects or markings.")
        if "ocel" not in code:
            raise ValueError("OCEL tool should use `ocel` as its primary input.")


def validate_assembled_code(code: str, category: str) -> None:
    if "def main" not in code:
        raise ValueError("Assembled code must define main().")

    forbidden_common = [
        "pm4py.read_xes(",
        "pm4py.read_ocel2_sqlite(",
        ProjectConfig.INPUT_XES_GENERAL,
        ProjectConfig.INPUT_SQLITE_OCEL,
    ]
    common_hits = [pattern for pattern in forbidden_common if pattern in code]
    if common_hits:
        raise ValueError(f"Assembled code must reuse ACTIVE_DATA instead of reloading files: {common_hits}")

    expected_binding = "event_log = ACTIVE_DATA" if category == "general" else "ocel = ACTIVE_DATA"
    if expected_binding not in code:
        raise ValueError(f"Assembled code must bind the runtime input with `{expected_binding}`.")

    if category == "general":
        invalid_hits = [pattern for pattern in ["ocel.events", "ocel.objects", "ocel.relations"] if pattern in code]
        if invalid_hits:
            raise ValueError(f"General assembled code uses OCEL-only patterns: {invalid_hits}")
    else:
        if "ocel['" in code or 'ocel["' in code:
            raise ValueError("Assembled OCEL code must use attribute access such as `ocel.events`.")
        if re.search(r"\bocel\.(get_object_ids|get_event_ids_by_object_ids|filter_events|filter_objects)\s*\(", code):
            raise ValueError(
                "Assembled OCEL code cannot call invented OCEL instance methods like "
                "`get_object_ids`, `get_event_ids_by_object_ids`, or `filter_events`; use "
                "`ocel.relations` / `ocel.objects` / `ocel.events` and `pm4py.filter_ocel_events(...)`."
            )
        if any(pattern in code for pattern in ["pm4py.ocel_filter(", "pm4py.ocel_propagate(", "pm4py.ocel_restrict("]):
            raise ValueError("Assembled OCEL code cannot call invented APIs like `pm4py.ocel_filter`, `pm4py.ocel_propagate`, or `pm4py.ocel_restrict`.")
        if "exact_object_type=" in code:
            raise ValueError("Assembled OCEL code must call `pm4py.ocel_flattening(ocel, \"orders\")` or use `object_type=\"orders\"`; `exact_object_type=` is not a valid PM4Py argument.")
        if 'ocdfg["nodes"]' in code or "ocdfg['nodes']" in code:
            raise ValueError('Assembled OCDFG code must use `len(ocdfg["activities"])`, not a non-existent `nodes` key.')
        if "pm4py.ocel_flattening(" in code and "case_id" in code:
            raise ValueError("Assembled flattened OCEL code must use `case:concept:name`, not `case_id`.")
        if "pm4py.ocel_flattening(" in code and "ocel:activity" in code:
            raise ValueError("Assembled flattened OCEL code must use `concept:name`, not `ocel:activity`.")
        if re.search(r"(['\"](?:events|objects|relations)['\"]\s+(?:not\s+)?in\s+ocel|\battr\s+in\s+ocel\b|\bin\s+ocel\s+for\s+attr\b)", code):
            raise ValueError("Assembled OCEL code must use attribute access on OCEL objects, not dictionary-membership checks or `in ocel` membership tests.")
        if "event_couples" in code and "DataFrame(top_edges, columns=['Edge', 'Frequency'])" in code:
            raise ValueError("Assembled OCDFG edge tables must flatten `event_couples` into row dictionaries before dataframe conversion.")
        if re.search(r"pm4py\.discover_ocdfg\(\s*[^)]*(flat|flatten|sublog|model_building)", code):
            raise ValueError("Assembled code cannot call `pm4py.discover_ocdfg` on flattened or filtered case-centric logs; use `pm4py.discover_dfg(flat_log)`.")
        if "pm4py.save_vis_ocdfg(" in code and "pm4py.ocel_flattening(" in code:
            raise ValueError("Assembled flattened DFG code must use `pm4py.save_vis_dfg(dfg, start_acts, end_acts, path)`.")
        if "pm4py.discover_dfg(" in code and any(
            pattern in code for pattern in ["dfg['edges']", 'dfg["edges"]', "dfg['activities']", 'dfg["activities"]', "dominant_variant", "average_case_duration"]
        ):
            raise ValueError("Assembled flattened DFG code must unpack `pm4py.discover_dfg(flat_log)` into `(dfg, start_acts, end_acts)`; `dfg` is not an OCDFG-style dict.")
        if any(pattern in code for pattern in [
            "ocpn.get(\"places\"",
            "ocpn.get('places'",
            "ocpn.get(\"transitions\"",
            "ocpn.get('transitions'",
            "ocpn.get(\"arcs\"",
            "ocpn.get('arcs'",
            "ocpn[\"places\"]",
            "ocpn['places']",
            "ocpn[\"transitions\"]",
            "ocpn['transitions']",
            "ocpn[\"arcs\"]",
            "ocpn['arcs']",
        ]):
            raise ValueError("Assembled OCPN code must summarize `ocpn['petri_nets']` per object type, not top-level places/transitions/arcs keys.")
        if re.search(r"pm4py\.discover_oc_petri_net\(\s*[^)]*(flat|flatten|sublog|model_building)", code):
            raise ValueError("Assembled code cannot call `pm4py.discover_oc_petri_net` on flattened or filtered case-centric logs; use `pm4py.discover_petri_net_inductive(flat_log)`.")
        if "pm4py.save_vis_ocpn(" in code and "pm4py.ocel_flattening(" in code:
            raise ValueError("Assembled flattened Petri-net code must use `pm4py.save_vis_petri_net(net, im, fm, path)`.")
        if "pm4py.replay_log(" in code and "pm4py.discover_petri_net_inductive(" in code:
            raise ValueError("Assembled flattened Petri-net conformance must use token-based replay helpers, not `pm4py.replay_log(...)`.")
        if "json.dump(ocdfg" in code or "json.dump(ocpn" in code:
            raise ValueError("Assembled code must not JSON-dump raw OCDFG/OCPN objects.")
        if "print(json.dumps(result" in code and any(token in code for token in ['"ocpn"', '"petri_net"', '"net"', '"im"', '"fm"']):
            raise ValueError("Assembled code must not JSON-serialize result dictionaries that still contain raw PM4Py objects or markings.")


def truncate_for_retry_prompt(value: Any, limit: int = 6000) -> str:
    if value is None:
        return "N/A"
    text = str(value).strip()
    if not text:
        return "N/A"
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n... (truncated, total {len(text)} chars)"


def get_previous_retry_artifacts(state: AgentState) -> Tuple[str, str]:
    previous_tool_code = state.get("last_generated_tool_code", "").strip()
    if not previous_tool_code and state.get("tool_code_list"):
        previous_tool_code = "\n\n".join(state["tool_code_list"]).strip()

    previous_executed_code = state.get("last_failed_code", "").strip()
    if not previous_executed_code:
        previous_executed_code = state.get("final_code", "").strip()

    return previous_tool_code, previous_executed_code


def infer_retry_failure_stage(state: AgentState) -> str:
    error_text = str(state.get("error", "")).strip()
    if error_text.startswith("Supervisor JSON Parse Error"):
        return "supervisor"
    if error_text.startswith("Assembler validation failed"):
        return "assembler_validation"
    if state.get("last_failed_code"):
        return "execution"
    if state.get("last_generated_tool_code"):
        return "tool_generation"
    return "unknown"


def infer_retry_root_cause(error_text: str, category: str) -> str:
    if not error_text:
        return "Unknown. The next plan must still change the prior assumption explicitly."
    if "'nodes'" in error_text:
        return "The previous attempt assumed an OC-DFG schema that PM4Py does not return."
    if "object_type_column" in error_text:
        return "The previous attempt mixed flattened case-centric data with raw-OCEL APIs or used the wrong flattening signature."
    if any(token in error_text for token in ["get_object_ids", "get_event_ids_by_object_ids", "filter_events"]):
        return "The previous attempt relied on OCEL instance methods that do not exist in this runtime."
    if any(token in error_text for token in ["ocel_filter", "ocel_propagate", "ocel_restrict"]):
        return "The previous attempt invented PM4Py OCEL helper names instead of using supported filtering patterns."
    if "not iterable" in error_text and "OCEL" in error_text:
        return "The previous attempt treated the OCEL object like a dict or iterable rather than an object with dataframe attributes."
    if "tuple indices must be integers or slices" in error_text:
        return "The previous attempt misread a tuple-returning PM4Py API as a dict-like structure."
    if "exact_object_type" in error_text:
        return "The previous attempt used an unsupported flattening keyword instead of PM4Py's real `object_type` argument."
    if category == "general" and "KeyError" in error_text:
        return "The previous attempt likely referenced a non-existent event-log column or activity label."
    return "The previous attempt violated the runtime API or data-shape assumptions, so the next plan must change those assumptions explicitly."


def build_retry_directives(state: AgentState, category: str) -> List[str]:
    directives = [
        "Study the previous error, plan, and code before proposing a new solution.",
        "Do not repeat the same failing API usage, column names, joins, or function signatures.",
        "If the previous attempt made a wrong assumption, explicitly change the approach instead of paraphrasing it.",
        "The new plan must say how it avoids the exact failure above.",
    ]

    error_text = str(state.get("error", ""))
    if category == "general":
        directives.extend([
            "For BPI 2017, stay within `event_log` and exact columns such as `concept:name`, `time:timestamp`, `org:resource`, `case:concept:name`.",
            "Never switch into OCEL accessors for a case-centric event log retry.",
        ])
    else:
        directives.extend([
            "For OCEL retries, use `ocel.events`, `ocel.objects`, `ocel.relations` and exact OCEL columns such as `ocel:activity`.",
            "Prefer stable high-level OCEL APIs like `pm4py.discover_ocdfg(ocel)` if the prior attempt failed on low-level access or signatures.",
        ])
        if "'nodes'" in error_text:
            directives.append("Use `ocdfg['activities']` and `ocdfg['edges']['event_couples']`; never access `ocdfg['nodes']`.")
        if "object_type_column" in error_text:
            directives.extend([
                "`pm4py.ocel_flattening(...)` returns a case-centric dataframe, so switch to case-centric APIs after flattening.",
                "For flattened DFG discovery, use `pm4py.discover_dfg(flat_log)` and `pm4py.save_vis_dfg(...)`; never call `pm4py.discover_ocdfg(flat_log)`.",
            ])
        if any(token in error_text for token in ["get_object_ids", "get_event_ids_by_object_ids", "filter_events"]):
            directives.extend([
                "Do not call invented OCEL instance methods like `ocel.get_object_ids(...)`, `ocel.get_event_ids_by_object_ids(...)`, or `ocel.filter_events(...)`.",
                "Derive event/object subsets from `ocel.relations`, then use `pm4py.filter_ocel_events(ocel, event_ids)` if available, or rebuild a restricted OCEL from filtered `events`, `objects`, and `relations`.",
                "To require events linked to multiple object types, group `ocel.relations` by `ocel:eid` and check the set of linked `ocel:type` values.",
            ])
        if any(token in error_text for token in ["ocel_filter", "ocel_propagate", "ocel_restrict"]):
            directives.extend([
                "Do not invent PM4Py OCEL helper names like `pm4py.ocel_filter(...)`, `pm4py.ocel_propagate(...)`, or `pm4py.ocel_restrict(...)`.",
                "For propagated event filtering, prefer `pm4py.filter_ocel_events(ocel, event_ids)`.",
                "If manual restriction is needed, use `copy.deepcopy(ocel)` and replace `events`, `objects`, and `relations` with filtered copies.",
            ])
        if "not iterable" in error_text and "OCEL" in error_text:
            directives.extend([
                "OCEL objects are not iterable, so never write checks like `'events' in ocel` or `all(attr in ocel for attr in ...)`.",
                "Use direct attribute access such as `ocel.events`, `ocel.objects`, `ocel.relations` or `hasattr(ocel, 'events')`.",
            ])
        if "tuple indices must be integers or slices" in error_text:
            directives.extend([
                "`pm4py.discover_dfg(flat_log)` returns a tuple `(dfg, start_acts, end_acts)`.",
                "Unpack it explicitly and treat `dfg` as a dict from `(source, target)` to frequency; do not access `dfg['edges']` or other OCDFG-style keys.",
            ])
        if "exact_object_type" in error_text:
            directives.append("Use `pm4py.ocel_flattening(ocel, \"packages\")` or `object_type=\"packages\"`, never `exact_object_type=...`.")

    return directives


def build_retry_corrective_signal(state: AgentState, category: str) -> str:
    if not state.get("error"):
        return ""

    previous_tool_code, previous_executed_code = get_previous_retry_artifacts(state)
    error_text = str(state.get("error", ""))
    directives = build_retry_directives(state, category)
    directive_block = "\n".join(f"{idx}. {directive}" for idx, directive in enumerate(directives, start=1))

    return f"""
        [Retry Corrective Signal]
        Failure Stage:
        {infer_retry_failure_stage(state)}

        Observed Error:
        {truncate_for_retry_prompt(error_text, limit=2500)}

        Root Cause Hypothesis:
        {infer_retry_root_cause(error_text, category)}

        Previous Failed Plan:
        Tool Plan:
        {truncate_for_retry_prompt(state.get('tool_plan'), limit=1500)}

        Analysis Plan:
        {truncate_for_retry_prompt(state.get('analysis_plan'), limit=1500)}

        Required Changes For The New Plan:
        {directive_block}

        Code Reuse Policy:
        Reuse only safe scaffolding. Rewrite the failing API calls, data-access pattern, and affected control flow instead of copying them forward unchanged.

        Previous Tool Code Snapshot:
        ```python
        {truncate_for_retry_prompt(previous_tool_code, limit=3000)}
        ```

        Previous Executed Script Snapshot:
        ```python
        {truncate_for_retry_prompt(previous_executed_code, limit=3000)}
        ```
        """


def build_retry_feedback(state: AgentState, category: str) -> str:
    if not state.get("error"):
        return ""

    previous_tool_code, previous_executed_code = get_previous_retry_artifacts(state)
    retry_guidance = "\n".join(
        f"        - {directive}" for directive in build_retry_directives(state, category)
    )

    return f"""
        [CRITICAL: PREVIOUS ATTEMPT FAILED]
        Error Message:
        {truncate_for_retry_prompt(state.get('error'), limit=4000)}

        Previous Tool Plan:
        {truncate_for_retry_prompt(state.get('tool_plan'), limit=4000)}

        Previous Analysis Plan:
        {truncate_for_retry_prompt(state.get('analysis_plan'), limit=4000)}

        Previous Generated Tool Code:
        ```python
        {truncate_for_retry_prompt(previous_tool_code, limit=8000)}
        ```

        Previous Executed Script:
        ```python
        {truncate_for_retry_prompt(previous_executed_code, limit=8000)}
        ```

        Retry Directive:
        {retry_guidance}
        """


def load_query_file(path: str) -> pd.DataFrame:
    """Query-file loader supporting both xlsx and csv (based on the 5-4 code)."""
    suffix = Path(path).suffix.lower()
    if suffix in [".xlsx", ".xls"]:
        xls = pd.ExcelFile(path)
        frames = []
        for sheet_name in xls.sheet_names:
            temp = pd.read_excel(path, sheet_name=sheet_name)
            lowered = {str(c).strip().lower() for c in temp.columns}
            has_query = any(c in lowered for c in ["query", "질의", "question"])
            if has_query:
                temp["__sheet_name__"] = sheet_name
                frames.append(temp)
        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.read_excel(path)
    return pd.read_csv(path)


def get_query_text(row: pd.Series):
    """Extract query text with compatibility across multiple column names."""
    for col in ["query", "질의", "question", "Query"]:
        if col in row and pd.notna(row.get(col)):
            q = str(row.get(col)).strip()
            if q:
                return q
    return None


def get_env_setting(*keys: str) -> str:
    for key in keys:
        value = os.getenv(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def parse_query_no_value(query_no: str) -> Union[int, None]:
    if not query_no:
        return None
    try:
        return int(float(str(query_no).strip()))
    except Exception:
        return None


def build_query_output_prefix(query_no: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_-]+", "_", str(query_no).strip())
    normalized = normalized.strip("_") or "unknown"
    return f"query_{normalized}_"


def prefix_output_filename(path_value: str, query_no: str) -> str:
    if not path_value:
        return path_value
    prefix = build_query_output_prefix(query_no)
    normalized_path = path_value.replace("\\", "/")
    directory, filename = os.path.split(normalized_path)
    if not filename or filename.startswith(prefix):
        return path_value
    return os.path.join(directory, prefix + filename) if directory else prefix + filename


def rewrite_query_output_paths(code: str, query_no: str) -> str:
    if not query_no:
        return code

    def replace_literal(match: re.Match) -> str:
        quote = match.group(1)
        literal = match.group(2)
        normalized = literal.replace("\\", "/")
        basename = os.path.basename(normalized).lower()
        is_relative_artifact_name = "/" not in normalized and not os.path.isabs(normalized)
        looks_like_output = (
            normalized.startswith("output/")
            or "/output/" in normalized
            or is_relative_artifact_name
            or basename in {"result.csv", "result.json", "result.pkl", "result.pickle", "result.txt", "result.html"}
        )
        if not looks_like_output:
            return match.group(0)
        updated = prefix_output_filename(literal, query_no)
        return f"{quote}{updated}{quote}"

    return re.sub(
        r"([\"'])((?:[^\"'\\]|\\.)+?\.(?:csv|json|pkl|pickle|png|svg|pdf|txt|html|gv))\1",
        replace_literal,
        code,
    )


def snapshot_output_artifacts(directory: str) -> Dict[str, Tuple[int, int]]:
    root = Path(directory)
    if not root.exists():
        return {}

    snapshot: Dict[str, Tuple[int, int]] = {}
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        snapshot[str(path.resolve())] = (stat.st_mtime_ns, stat.st_size)
    return snapshot


def build_unique_output_path(path: Path) -> Path:
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    counter = 2
    while True:
        candidate = path.with_name(f"{stem}_{counter}{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def format_artifact_path_for_log(path_value: Union[str, Path]) -> str:
    path = Path(path_value).resolve()
    try:
        return str(path.relative_to(Path(ProjectConfig.BASE_DIR).resolve())).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def prefix_new_output_artifacts(
    output_dir: str,
    query_no: str,
    previous_snapshot: Dict[str, Tuple[int, int]],
) -> List[Tuple[str, str]]:
    if not query_no:
        return []

    prefix = build_query_output_prefix(query_no)
    current_snapshot = snapshot_output_artifacts(output_dir)
    renamed_paths: List[Tuple[str, str]] = []

    for path_str, metadata in sorted(current_snapshot.items()):
        if previous_snapshot.get(path_str) == metadata:
            continue

        source_path = Path(path_str)
        if source_path.name.startswith(prefix):
            continue

        target_name = prefix_output_filename(source_path.name, query_no)
        if target_name == source_path.name:
            continue

        target_path = build_unique_output_path(source_path.with_name(target_name))
        try:
            source_path.rename(target_path)
            renamed_paths.append(
                (
                    format_artifact_path_for_log(source_path),
                    format_artifact_path_for_log(target_path),
                )
            )
        except OSError as exc:
            print(f"[Artifact Rename Warning] {source_path} -> {target_path}: {exc}")

    return renamed_paths


# --------------------------------------------------
# 4. Supervisor Node
# --------------------------------------------------
def supervisor_node(state: AgentState):
    LogUtils.node_start("Supervisor", state['retry_count'])
    category = state.get("category", "ocel")
    benchmark_context = build_benchmark_context(
        state.get("benchmark_query_type", ""),
        state.get("answer_format", ""),
        state.get("query_interpretation", ""),
    )
    
    # [Check 1] RAG retrieval for Supervisor: search broadly for overall context
    rag_docs = rag_manager.search_context(state['query'], k=5, filter_type="tool")

    # Log the retrieved RAG snippets
    LogUtils.print_rag_sources(rag_docs, title="RAG Context for Supervisor")

    rag_context = "\n".join([d.page_content for d, s in rag_docs])
    # print((f"   [DEBUG] RAG Retrieved {len(rag_docs)} chunks.")
    rich_context = state.get("rich_context", "")
    query_requirements = state.get("query_requirements", "")

    # 4. Error feedback
    error_context = build_retry_feedback(state, category)

    if category == "general":
        system_prompt = f"""
        You are a Lead Process Mining Architect specializing in PM4Py for case-centric event logs.
        Your goal is to break down the user query into a logical flow of steps grounded in the actual BPI Challenge 2017 dataset.

        [Context Info]
        {rich_context}

        {benchmark_context}

        {query_requirements}

        [RAG Knowledge (Reference Only)]
        {rag_context}

        {error_context}

        [Task]
        Analyze the user query: "{state['query']}"
        Design a robust 'Tool Plan' and 'Analysis Plan'.

        [Strict Architecture Rules - FOLLOW THESE OR FAIL]
        1. The data is a case-centric XES event log. Never use OCEL accessors such as `ocel.events`, `ocel.objects`, or `ocel.relations`.
        2. Use the exact BPI 2017 dataframe column names from the dataset context:
           - Activity: `concept:name`
           - Timestamp: `time:timestamp`
           - Resource: `org:resource`
           - Case id: `case:concept:name`
        3. Activity names must match the dataset exactly. If query-specific alias hints are provided, resolve human-friendly phrases to those exact dataset labels.
        4. For discovery/variants/performance/resource analysis, prefer PM4Py event-log APIs or Pandas on `pm4py.convert_to_dataframe(event_log)`.
        5. For conformance checking, explicitly mention the reference Petri net input and the event-log subset if the query asks for one.
        6. The plan must mention the real input as `event_log` (and `log_df` only if needed).
        7. The plan must satisfy the parsed benchmark answer contract: result_type, view, and result_schema keys.

        [Output JSON Format]
        {{
            "query_type": "Case-Centric",
            "tool_plan": "Detailed description of the Python function. Explicitly mention inputs (event_log / optional log_df) and outputs/artifacts.",
            "analysis_plan": "Step-by-step logic grounded in the exact BPI columns such as concept:name, time:timestamp, org:resource, case:concept:name."
        }}
        """
    else:
        system_prompt = f"""
        You are a Lead Process Mining Architect specializing in PM4Py for object-centric event logs (OCEL 2.0).
        Your goal is to break down the user query into a logical flow of steps grounded in the actual order-management dataset.

        [Context Info]
        {rich_context}

        {benchmark_context}

        {query_requirements}

        [RAG Knowledge (Reference Only)]
        {rag_context}

        {error_context}

        [Task]
        Analyze the user query: "{state['query']}"
        Design a robust 'Tool Plan' and 'Analysis Plan'.

        [Strict Architecture Rules - FOLLOW THESE OR FAIL]
        1. The data is an OCEL 2.0 object, not a dictionary.
           - WRONG: `ocel['events']`, `ocel['objects']`
           - CORRECT: `ocel.events`, `ocel.objects`, `ocel.relations`
           - ALSO WRONG: `ocel.get_object_ids(...)`, `ocel.get_event_ids_by_object_ids(...)`, `ocel.filter_events(...)`
           - ALSO WRONG: `pm4py.ocel_filter(...)`, `pm4py.ocel_propagate(...)`, `pm4py.ocel_restrict(...)`
        2. Use the exact OCEL column names from the dataset context:
           - Event activity: `ocel:activity`
           - Event timestamp: `ocel:timestamp`
           - Event id: `ocel:eid`
           - Object id: `ocel:oid`
           - Object type: `ocel:type`
           - Qualifier: `ocel:qualifier`
        3. Activity and object-type strings must match the real order-management dataset exactly. Do not invent names or flattened views that are not in the dataset context.
        4. For stats/counts use Pandas on `ocel.events` / `ocel.objects` / `ocel.relations`.
        5. For discovery use OCEL-specific PM4Py APIs such as `pm4py.discover_ocdfg(ocel)` or `pm4py.discover_oc_petri_net(ocel)`.
        6. To link events to objects, merge carefully on `ocel:eid` after selecting only needed columns.
        7. If the query requires a flattened view, explicitly create a separate flattened event log with `pm4py.ocel_flattening(ocel_or_restricted_ocel, "orders")` or `pm4py.ocel_flattening(ocel_or_restricted_ocel, object_type="orders")`, using the exact dataset object type, and then use case-centric columns on that flattened log.
        8. If the query mixes raw OCEL and flattened-view reasoning, keep them as separate variables such as `ocel` and `flat_log`; never mix `ocel:*` columns with `concept:name` / `time:timestamp` in the same dataframe without stating the conversion step.
        9. If the query says "propagate the filter", the plan must first build a restricted raw OCEL and only then run downstream discovery or flattening on that restricted OCEL.
        10. The plan must satisfy the parsed benchmark answer contract: result_type, view, and result_schema keys.
        11. `pm4py.discover_ocdfg(ocel)` returns a dictionary whose node-like summary is the `activities` set; edge details live under `ocdfg['edges']['event_couples']`.
        12. `pm4py.discover_oc_petri_net(ocel)` returns a dictionary with `petri_nets` keyed by object type, where each value is `(net, initial_marking, final_marking)`.
        13. `pm4py.ocel_flattening(...)` returns a case-centric dataframe. Call it with the real PM4Py signature such as `pm4py.ocel_flattening(ocel, "packages")` or `pm4py.ocel_flattening(ocel, object_type="packages")`, not invented keyword names, and after flattening use case-centric discovery APIs such as `pm4py.discover_petri_net_inductive(flat_log)`, not OCEL-only discovery APIs.
        14. Flattened dataframe columns are case-centric: `case:concept:name`, `concept:name`, `time:timestamp`, plus optional `case:*` columns. Do not expect `case_id` or `ocel:activity` after flattening.
        15. For flattened DFG discovery, use case-centric PM4Py APIs such as `pm4py.discover_dfg(flat_log)` and `pm4py.save_vis_dfg(dfg, start_acts, end_acts, path)`. Never call `pm4py.discover_ocdfg(flat_log)`.
        16. `pm4py.discover_dfg(flat_log)` returns `(dfg, start_acts, end_acts)`, where `dfg` is a dict keyed by `(source, target)` activity pairs. Do not treat it like an OCDFG dict with `edges`, `activities`, or `dominant_variant`.
        17. If the query asks for top-k or top-20%-variant subsets on a flattened log, variants are case-level tuples derived via `groupby("case:concept:name")["concept:name"].agg(tuple).value_counts()`, not simple activity frequencies.
        18. For flattened Petri-net conformance, use `pm4py.fitness_token_based_replay(...)` and `pm4py.conformance_diagnostics_token_based_replay(...)`, not `pm4py.replay_log(...)`.

        [Output JSON Format]
        {{
            "query_type": "Object-Centric",
            "tool_plan": "Detailed description of the Python function. Explicitly mention inputs (ocel) and outputs/artifacts.",
            "analysis_plan": "Step-by-step logic grounded in ocel.events / ocel.objects / ocel.relations and exact order-management schema values."
        }}
        """
    
    try:
        response = llm_supervisor.invoke([("system", system_prompt), ("human", state['query'])])
        result = clean_and_parse_json(response.content)
        
        LogUtils.info("Tool Plan", result['tool_plan'], truncate_len=5000)
        LogUtils.info("Analysis Plan", result['analysis_plan'], truncate_len=5000)
        
        return {
            "tool_plan": result['tool_plan'], 
            "analysis_plan": result['analysis_plan'], 
            "rich_context": rich_context, 
            "error": None,
            "tool_code_list": []
        }
    except Exception as e:
        LogUtils.error("Supervisor Error", str(e))
        return {
            "error": f"Supervisor JSON Parse Error: {str(e)}", 
            "retry_count": state['retry_count'] + 1
        }

# --------------------------------------------------
# 5. Tool Generator Node
# --------------------------------------------------
def tool_generator_node(state: AgentState):
    LogUtils.node_start("Tool Generator")
    category = state.get("category", "ocel")
    benchmark_context = build_benchmark_context(
        state.get("benchmark_query_type", ""),
        state.get("answer_format", ""),
        state.get("query_interpretation", ""),
    )
    query_requirements = state.get("query_requirements", "")
    retry_signal = build_retry_corrective_signal(state, category)
    
    tools = []
    cached_code = rag_manager.check_tool_cache(state['tool_plan'])
    
    if cached_code:
        try:
            validate_generated_tool_code(cached_code, category)
            tools.append(cached_code)
            LogUtils.code_snippet("Cached Tool Code", cached_code)
            return {
                "tool_code_list": [cached_code],
                "last_generated_tool_code": cached_code
            }
        except Exception as exc:
            LogUtils.info("Cached Tool Rejected", exc)

    # print("   -> [Miss] Generating New Tool.")
    # The generator needs implementation-oriented material, so search only tools
    manual_docs = rag_manager.search_context(state['tool_plan'], k=5, filter_type="tool")

    # Log the retrieved manual snippets
    LogUtils.print_rag_sources(manual_docs, title="RAG Manual for Tool Generator")

    manual_context = "\n".join([d.page_content for d, s in manual_docs])

    # [Fix] Get the prefix that already includes the current timestamp
    prefix = ProjectConfig.FILE_PREFIX

    if category == "general":
        compatibility_map = f"""
        [REFERENCE PM4PY CODE PATTERNS - CASE-CENTRIC EVENT LOG]

        # 1. Safe DataFrame Conversion
        log_df = pm4py.convert_to_dataframe(event_log)
        log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])

        # 2. Start / End Activities
        ordered = log_df.sort_values(['case:concept:name', 'time:timestamp'])
        start_activities = ordered.groupby('case:concept:name')['concept:name'].first().value_counts().to_dict()
        end_activities = ordered.groupby('case:concept:name')['concept:name'].last().value_counts().to_dict()

        # 3. DFG Discovery + Visualization
        dfg, start_acts, end_acts = pm4py.discover_dfg(event_log)
        path = "output/{prefix}_dfg.png"
        pm4py.save_vis_dfg(dfg, start_acts, end_acts, path)
        print(f"OUTPUT_FILE_LOCATION: {{path}}")

        # 4. Petri Net Discovery
        net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(event_log)
        print(len(net.places), len(net.transitions), len(net.arcs))

        # 5. Variant Analysis
        ordered = log_df.sort_values(['case:concept:name', 'time:timestamp'])
        variants = ordered.groupby('case:concept:name')['concept:name'].agg(tuple).value_counts()
        print(variants.head(10))

        # 6. Throughput / Sojourn Skeleton
        case_times = ordered.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
        case_times['throughput_seconds'] = (case_times['max'] - case_times['min']).dt.total_seconds()

        # 7. Resource Frequency
        if 'org:resource' in log_df.columns:
            print(log_df['org:resource'].dropna().value_counts().head(10))
        """

        implementation_rules = f"""
        [CRITICAL IMPLEMENTATION RULES - DO NOT IGNORE]
        1. Use `event_log` as the primary input variable. Create `log_df = pm4py.convert_to_dataframe(event_log)` only when needed.
        2. Never use OCEL accessors such as `ocel.events`, `ocel.objects`, or `ocel.relations`.
        3. Use the exact BPI 2017 columns:
           - `concept:name`
           - `time:timestamp`
           - `org:resource`
           - `case:concept:name`
        4. Activity labels are exact BPI strings with prefixes like `A_`, `O_`, `W_`. Do not strip prefixes or invent simplified spellings.
        5. If the query asks for variants, top-k cases, or start/end activities, sort by `case:concept:name` and `time:timestamp` before grouping.
        6. If saving images or files, save them under `output/...`, prefix filenames with `query_<query_no>_` first and then `{ProjectConfig.FILE_PREFIX}_` when you choose the filename, and print `OUTPUT_FILE_LOCATION: ...` after each save.
        7. Never print huge objects; use `head(20)`, `len(...)`, or concise dictionaries/lists.
        8. Prefer PM4Py high-level APIs for DFG, Petri net, conformance, and visualization. Use Pandas when the query asks for explicit tables or counts.
        9. Do not invent columns that do not exist in the BPI dataset.
        """
    else:
        compatibility_map = f"""
        [REFERENCE PM4PY CODE PATTERNS - OBJECT-CENTRIC EVENT LOG]

        # 1. Object-Centric DFG (Discovery & Vis)
        ocdfg = pm4py.discover_ocdfg(ocel) 
        pm4py.save_vis_ocdfg(ocdfg, "output/{prefix}_ocdfg.png", annotation='frequency')
        num_nodes = len(ocdfg.get("activities", []))
        edge_map = (ocdfg.get("edges") or {{}}).get("event_couples", {{}})
        edge_rows = []
        for object_type, couples in edge_map.items():
            for (source, target), linked_pairs in couples.items():
                edge_rows.append({{
                    "object_type": object_type,
                    "source": source,
                    "target": target,
                    "frequency": len(linked_pairs),
                }})
        top_edges_df = pd.DataFrame(edge_rows).sort_values("frequency", ascending=False).head(10)
        print(f"Number of object-type edge tables: {{len(edge_map)}}")

        # 2. Object-Centric Petri Net (Discovery & Vis)
        ocpn = pm4py.discover_oc_petri_net(ocel)
        pm4py.save_vis_ocpn(ocpn, "output/{prefix}_ocpn.png")
        petri_nets = ocpn.get("petri_nets", {{}})
        per_type_stats = {{
            object_type: {{
                "places": len(net.places),
                "transitions": len(net.transitions),
                "arcs": len(net.arcs),
            }}
            for object_type, (net, initial_marking, final_marking) in petri_nets.items()
        }}

        # 3. Object Type Graph (OTG)
        ot_set, edges_dict = pm4py.discover_otg(ocel)
        sorted_edges = sorted(edges_dict.items(), key=lambda x: x[1], reverse=True)[:5]
        print(f"Top 5 Strongest Edges: {{sorted_edges}}")

        # 4. Object Interaction Graph (OIG)
        graph = pm4py.discover_objects_graph(ocel)
        pm4py.save_vis_object_graph(ocel, graph, "output/{prefix}_oig.png")
        print(f"Number of interacting object pairs: {{len(graph)}}")

        # 5. Safe Column Access
        events = ocel.events
        if 'ocel:activity' in events.columns:
             print(events['ocel:activity'].value_counts().head(5))

        # 6. Safe Object-Event Linking
        events = ocel.events[['ocel:eid', 'ocel:activity', 'ocel:timestamp']]
        relations = ocel.relations[['ocel:eid', 'ocel:oid', 'ocel:type']]
        merged = pd.merge(events, relations, on='ocel:eid')
        if 'ocel:type' in merged.columns:
             print(merged.groupby('ocel:type')['ocel:oid'].nunique().head())

        # 7. Lead Time / Duration Skeleton
        events = ocel.events[['ocel:eid', 'ocel:activity', 'ocel:timestamp']]
        relations = ocel.relations[['ocel:eid', 'ocel:oid']]
        target_events = events[events['ocel:activity'].isin(['place order', 'package delivered'])]
        merged = pd.merge(target_events, relations, on='ocel:eid')

        # 8. Restricted OCEL by Event Ids
        joint_event_ids = set(merged['ocel:eid'].astype(str))
        if hasattr(pm4py, 'filter_ocel_events'):
            restricted_ocel = pm4py.filter_ocel_events(ocel, sorted(joint_event_ids))
        else:
            restricted_rel = ocel.relations[ocel.relations['ocel:eid'].astype(str).isin(joint_event_ids)].copy()
            restricted_object_ids = set(restricted_rel['ocel:oid'].astype(str))
            restricted_ocel = copy.deepcopy(ocel)
            restricted_ocel.events = ocel.events[ocel.events['ocel:eid'].astype(str).isin(joint_event_ids)].copy()
            restricted_ocel.objects = ocel.objects[ocel.objects['ocel:oid'].astype(str).isin(restricted_object_ids)].copy()
            restricted_ocel.relations = restricted_rel

        # 8b. Restricted OCEL by Required Object Types
        required_types = {{"items", "customers"}}
        rel_types = ocel.relations[["ocel:eid", "ocel:type", "ocel:oid"]].copy()
        linked_types = rel_types.groupby("ocel:eid")["ocel:type"].agg(lambda values: set(values.dropna()))
        joint_event_ids = sorted(
            str(event_id)
            for event_id, object_types in linked_types.items()
            if required_types.issubset(object_types)
        )
        if hasattr(pm4py, "filter_ocel_events"):
            restricted_ocel = pm4py.filter_ocel_events(ocel, joint_event_ids)
        else:
            restricted_rel = ocel.relations[ocel.relations["ocel:eid"].astype(str).isin(joint_event_ids)].copy()
            restricted_object_ids = set(restricted_rel["ocel:oid"].astype(str))
            restricted_ocel = copy.deepcopy(ocel)
            restricted_ocel.events = ocel.events[ocel.events["ocel:eid"].astype(str).isin(joint_event_ids)].copy()
            restricted_ocel.objects = ocel.objects[ocel.objects["ocel:oid"].astype(str).isin(restricted_object_ids)].copy()
            restricted_ocel.relations = restricted_rel

        # 8c. Flattened top-20%-variant subset
        flat_log = pm4py.ocel_flattening(restricted_ocel, "customers")
        ordered = flat_log.sort_values(["case:concept:name", "time:timestamp"])
        variant_counts = ordered.groupby("case:concept:name")["concept:name"].agg(tuple).value_counts()
        top_variant_count = max(1, math.ceil(len(variant_counts) * 0.2))
        top_variants = set(variant_counts.head(top_variant_count).index)
        case_variants = ordered.groupby("case:concept:name")["concept:name"].agg(tuple)
        selected_case_ids = case_variants[case_variants.isin(top_variants)].index
        model_sublog = ordered[ordered["case:concept:name"].isin(selected_case_ids)].copy()

        # 9. Flattened View -> Case-Centric Petri Net
        flat_log = pm4py.ocel_flattening(ocel, "packages")
        net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(flat_log)
        pm4py.save_vis_petri_net(net, initial_marking, final_marking, "output/{prefix}_im_packages.png")
        print(len(net.places), len(net.transitions), len(net.arcs))

        # 10. Flattened View -> Variants / Top-k subset
        flat_log = pm4py.ocel_flattening(ocel, "customers")
        ordered = flat_log.sort_values(["case:concept:name", "time:timestamp"])
        variant_counts = ordered.groupby("case:concept:name")["concept:name"].agg(tuple).value_counts()
        top_variant_count = max(1, math.ceil(len(variant_counts) * 0.2))
        top_variants = set(variant_counts.head(top_variant_count).index)
        case_variants = ordered.groupby("case:concept:name")["concept:name"].agg(tuple)
        selected_case_ids = case_variants[case_variants.isin(top_variants)].index
        model_sublog = ordered[ordered["case:concept:name"].isin(selected_case_ids)].copy()

        # 11. Flattened View -> Token-based replay
        net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(model_sublog)
        fitness_summary = pm4py.fitness_token_based_replay(model_sublog, net, initial_marking, final_marking)
        diagnostics = pm4py.conformance_diagnostics_token_based_replay(model_sublog, net, initial_marking, final_marking)
        fit_rate = (
            sum(1 for item in diagnostics if item.get("trace_is_fit")) / len(diagnostics)
            if diagnostics else 0.0
        )

        # 12. Flattened View -> Sojourn time
        flat_log = pm4py.ocel_flattening(ocel, "orders")
        ordered = flat_log.sort_values(["case:concept:name", "time:timestamp"]).copy()
        ordered["next_timestamp"] = ordered.groupby("case:concept:name")["time:timestamp"].shift(-1)
        ordered["sojourn_seconds"] = (
            ordered["next_timestamp"] - ordered["time:timestamp"]
        ).dt.total_seconds()
        mean_sojourn = ordered.groupby("concept:name")["sojourn_seconds"].mean().dropna().to_dict()

        # 13. Flattened View -> Case-Centric DFG
        flat_log = pm4py.ocel_flattening(ocel, "orders")
        dfg, start_acts, end_acts = pm4py.discover_dfg(flat_log)
        dfg_rows = [
            {{"source": source, "target": target, "frequency": frequency}}
            for (source, target), frequency in dfg.items()
        ]
        dfg_edges_df = pd.DataFrame(dfg_rows).sort_values("frequency", ascending=False)
        pm4py.save_vis_dfg(dfg, start_acts, end_acts, "output/{prefix}_dfg_orders.png")
        """

        implementation_rules = f"""
        [CRITICAL IMPLEMENTATION RULES - DO NOT IGNORE]
        1. Pattern-match against the object-centric examples above whenever the plan is similar.
        2. No subscripting on OCEL:
           - WRONG: `ocel['events']`
           - CORRECT: `ocel.events`
        3. Use the exact order-management schema:
           - Event columns: `ocel:eid`, `ocel:timestamp`, `ocel:activity`
           - Object columns: `ocel:oid`, `ocel:type`, `role`, `weight`, `price`
           - Relation columns: `ocel:eid`, `ocel:oid`, `ocel:qualifier`, `ocel:type`
        4. The real order-management activity strings are lowercase phrases like `place order`, `pick item`, `confirm order`, `send package`.
        5. Do not invent unsupported object types or flattened views.
        6. When merging `ocel.events` and `ocel.relations`, select only needed columns first to avoid `_x` / `_y` suffix issues.
        7. If saving images or files, save them under `output/...`, prefix filenames with `query_<query_no>_` first and then `{ProjectConfig.FILE_PREFIX}_` when you choose the filename, and print `OUTPUT_FILE_LOCATION: ...` after each save.
        8. Do not invent PM4Py arguments; for example `pm4py.discover_ocdfg` takes only `ocel`.
        8a. Do not invent OCEL instance methods. In this environment, `ocel` exposes dataframe attributes like `events`, `objects`, `relations`, `object_changes`, `o2o`, `e2e`, but not helpers such as `get_object_ids`, `get_event_ids_by_object_ids`, or `filter_events`.
        8b. Do not invent PM4Py OCEL functions such as `pm4py.ocel_filter`, `pm4py.ocel_propagate`, or `pm4py.ocel_restrict`. For propagated filtering, use `pm4py.filter_ocel_events(ocel, event_ids)` if available; otherwise use `copy.deepcopy(ocel)` and replace `events`, `objects`, and `relations`.
        9. Never print huge objects; use concise summaries only.
        10. If the query requires a flattened view, use `pm4py.ocel_flattening(ocel_or_restricted_ocel, "orders")` or `pm4py.ocel_flattening(ocel_or_restricted_ocel, object_type="orders")` with the exact dataset object type, and then treat the flattened result as a case-centric event log.
        11. If the query mixes raw OCEL and flattened-view reasoning, keep the namespaces separate: raw OCEL uses `ocel:*` columns, flattened logs use `case:concept:name`, `concept:name`, `time:timestamp`.
        12. If the query says "propagate the filter", first construct a restricted raw OCEL by event ids and only then flatten or discover on that restricted OCEL.
        13. Never call undefined helpers such as `flatten_ocel_view`, `load_reference_ocpn`, or `pm4py.flatten_ocel`.
        14. Never `json.dump` raw `ocdfg` / `ocpn` objects directly because they contain sets and non-serializable values; serialize only concise summaries or normalized tables.
        15. For OC-DFG summary counts, use `len(ocdfg.get("activities", []))` and `ocdfg['edges']['event_couples']`; never assume a `nodes` key exists.
        16. For OCPN summary counts, inspect `ocpn['petri_nets']` and measure each `(net, initial_marking, final_marking)` tuple per object type.
        17. If a flattened view is used, the downstream model is a standard Petri net tuple `(net, initial_marking, final_marking)`, not an OCPN dictionary.
        18. `pm4py.discover_dfg(flat_log)` returns `(dfg, start_acts, end_acts)`. The first element is a dict of `(source, target) -> frequency`; do not index it with string keys like `edges`.
        19. If you print final results, print only JSON-serializable summaries such as `statistics_dict` or `__summary__`. Never `json.dumps(...)` a structure that still contains PM4Py objects, sets, or markings.
        20. Saving PNG visualizations is optional. If Graphviz or `dot` is unavailable, catch the exception, print a concise warning, and still return the main analytical result.
        21. For flattened variants, derive them via `flat_log.sort_values(["case:concept:name", "time:timestamp"]).groupby("case:concept:name")["concept:name"].agg(tuple).value_counts()`. Never treat activity frequency as variant frequency.
        22. For flattened token-based replay, use case-centric APIs such as `pm4py.discover_petri_net_inductive(flat_log)`, `pm4py.fitness_token_based_replay(flat_log, net, initial_marking, final_marking)`, or `pm4py.conformance_diagnostics_token_based_replay(...)`. Do not use `pm4py.replay_log(...)`.
        23. For flattened sojourn time, compute the next timestamp per `case:concept:name` and aggregate by `concept:name`.
        24. For flattened DFGs, use `pm4py.discover_dfg(flat_log)` and treat the result as edge-frequency pairs over `concept:name`; do not expect OCDFG fields like `event_couples`, `dominant_variant`, or `average_case_duration`.
        """

    gen_prompt = f"""
    You are a Senior Python Developer for PM4Py.
    Generate a Python function based on the plan.

    [Dataset Category]
    {category}

    [Context]
    {state.get('rich_context', 'No context')}

    {benchmark_context}

    {query_requirements}

    {retry_signal}

    [RAG Manual Snippets]
    {manual_context}

    [Plan]
    {state['tool_plan']}

    {compatibility_map}

    {implementation_rules}

    [Shared Rules]
    - Prefer top-level imports such as `import pm4py`, `import pandas as pd`, `import json`, `import os`, `import pickle` when needed.
    - Do not read the source data file inside the tool; the loaded data object is passed in.
    - The tool must return or print only concise JSON-serializable outputs; save large tables/files under `output/...`.
    - If a retry corrective signal is present, treat it as mandatory and change the failing approach explicitly.
    - Generate ONLY Python code.
    """

    try:
        code = llm_worker.invoke(gen_prompt).content.replace("```python", "").replace("```", "").strip()
        validate_generated_tool_code(code, category)
        LogUtils.code_snippet("Generated Tool Code", code)
        return {
            "tool_code_list": [code],
            "last_generated_tool_code": code
        }
    except Exception as e:
        return {"error": str(e)}


# --------------------------------------------------
# 6. Code Assembler Node
# --------------------------------------------------
def code_assembler_node(state: AgentState):
    LogUtils.node_start("Code Assembler")
    category = state.get("category", "ocel")
    benchmark_context = build_benchmark_context(
        state.get("benchmark_query_type", ""),
        state.get("answer_format", ""),
        state.get("query_interpretation", ""),
    )
    query_requirements = state.get("query_requirements", "")
    
    tool_block = "\n\n".join(state['tool_code_list'])
    if category == "general":
        data_load_instruction = "- Bind runtime input: `event_log = ACTIVE_DATA`"
        primary_input_name = "event_log"
    else:
        data_load_instruction = "- Bind runtime input: `ocel = ACTIVE_DATA`"
        primary_input_name = "ocel"
    
    # [Updated] Strengthened prompt: combine tools and plan into an executable script
    assembler_prompt = f"""
    You are a System Integrator. Assemble the final script.

    [Dataset Category]
    {category}

    [Data Summary]
    {state.get('data_summary', '')}

    {benchmark_context}

    {query_requirements}

    [Tools provided]
    {tool_block}

    [Logic]
    {state['analysis_plan']}

    [Requirements]
    1. Define `def main():`.
    2. Inside main:
       {data_load_instruction}
       - Call the tool functions using `{primary_input_name}` as the primary input.
       - Print concise results only.
       - **Ensure the tool function prints the 'OUTPUT_FILE_LOCATION' if a file is saved.**
    3. **Safety Check**:
       - Ensure `import pm4py` and `import pandas as pd` are present.
       - Wrap everything in `try: ... except Exception as e: print("EXECUTION_FAILURE:", e)`.
    4. Do not reload the source file path. The runtime dataset object is already available as `ACTIVE_DATA`.
    5. Use the dataset-specific access pattern:
       - `general` => event-log APIs / dataframe from `event_log`
       - `ocel` => OCEL APIs on `ocel`
    6. If a flattened OCEL view is needed, create it explicitly from `ocel` or a restricted OCEL object using the real PM4Py signature `pm4py.ocel_flattening(ocel, "orders")` or `pm4py.ocel_flattening(ocel, object_type="orders")`, and keep flattened-log columns separate from raw-OCEL columns.
    7. The assembled script must bind the runtime input exactly once via `event_log = ACTIVE_DATA` or `ocel = ACTIVE_DATA`.
    8. Preserve the real PM4Py return structures:
       - `discover_ocdfg(ocel)` => summarize via `ocdfg['activities']` and `ocdfg['edges']['event_couples']`
       - `discover_oc_petri_net(ocel)` => summarize via `ocpn['petri_nets']`
       - `ocel_flattening(...)` => dataframe, so use case-centric discovery like `discover_petri_net_inductive(flat_log)`
       - `discover_dfg(flat_log)` => `(dfg, start_acts, end_acts)` where `dfg` is a dict from `(source, target)` to frequency
    9. If the result dictionary still contains raw PM4Py objects, do not `json.dumps(result)`. Print only a concise summary string or a plain statistics dictionary.
    10. Visualization saves are best-effort only. Wrap `save_vis_*` calls in `try/except`; if Graphviz is unavailable, continue and print a concise warning instead of failing the whole query.
    11. Flattened OCEL dataframe columns are case-centric. Use `case:concept:name`, `concept:name`, and `time:timestamp`; never expect `case_id` or `ocel:activity` in `flat_log`.
    12. If the task is flattened DFG discovery, use `pm4py.discover_dfg(flat_log)` and `pm4py.save_vis_dfg(...)`; do not use raw-OCEL APIs like `pm4py.discover_ocdfg(flat_log)`.
    13. If the task requires propagated OCEL filtering, do not invent functions like `pm4py.ocel_filter`, `pm4py.ocel_propagate`, or `pm4py.ocel_restrict`; use `pm4py.filter_ocel_events(...)` or a restricted copy built from filtered dataframes.
    14. For flattened top-variant subsets, variants are case-level tuples, not `concept:name` frequency counts.
    15. For flattened conformance against a discovered Petri net, use token-based replay helpers, not `pm4py.replay_log(...)`.

    Return ONLY Python code.
    """
    
    final_code_response = llm_worker.invoke(assembler_prompt)
    final_code = final_code_response.content.replace("```python", "").replace("```", "").strip()
    final_code = rewrite_query_output_paths(final_code, state.get("query_no", ""))
    try:
        validate_assembled_code(final_code, category)
    except Exception as exc:
        return {
            "error": f"Assembler validation failed: {exc}",
            "last_failed_code": final_code,
            "execution_result": str(exc),
            "retry_count": state['retry_count'] + 1,
            "final_code": final_code
        }
    
    print(f"   - {LogUtils.BOLD}Assembled Main Code:{LogUtils.RESET}\n{LogUtils.YELLOW}{final_code}{LogUtils.RESET}")
    
    # --- Execution Sandbox ---
    buffer = io.StringIO()
    execution_error = None
    captured_output = ""
    output_snapshot = snapshot_output_artifacts(ProjectConfig.OUTPUT_IMAGE_DIR)
    renamed_artifacts: List[Tuple[str, str]] = []
    
    try:
        exec_globals = {
            "pm4py": pm4py, "pd": pd, "np": np, "json": json,
            "os": os, "pickle": pickle, "math": math, "statistics": statistics,
            "collections": collections, "copy": copy,
            "sqlite3": sqlite3, "plt": plt, "io": io, "sys": sys, "traceback": traceback,
            "ACTIVE_DATA": get_runtime_input(category),
        }
        
        exec(final_code, exec_globals)

        if "main" in exec_globals:
            with redirect_stdout(buffer):
                exec_globals["main"]()
            captured_output = buffer.getvalue().strip()
        else:
            raise ValueError("No main function found.")
            
    except Exception:
        execution_error = traceback.format_exc()

    renamed_artifacts = prefix_new_output_artifacts(
        ProjectConfig.OUTPUT_IMAGE_DIR,
        state.get("query_no", ""),
        output_snapshot,
    )
    if renamed_artifacts:
        rename_log = "\n".join(
            f"OUTPUT_FILE_LOCATION: {new_path}" for _, new_path in renamed_artifacts
        )
        captured_output = f"{captured_output}\n{rename_log}".strip() if captured_output else rename_log
        for old_path, new_path in renamed_artifacts:
            print(f"   [Artifact Rename] {old_path} -> {new_path}")
        
    # --- Error Detection ---
    is_logic_error = "EXECUTION_FAILURE" in captured_output or any(
        err in captured_output for err in ["Traceback", "TypeError", "AttributeError", "ImportError", "NameError", "KeyError"]
    )

    # --- Result Handling ---
    if execution_error or is_logic_error or not captured_output:
        final_err = execution_error if execution_error else captured_output
        LogUtils.error("Execution Failed (Not Saved)", details=final_err)
        
        return {
            "error": final_err,
            "last_failed_code": final_code,
            "execution_result": final_err,
            "retry_count": state['retry_count'] + 1,
            "final_code": final_code
        }
    else:
        # Success handling
        print(f"\n{LogUtils.GREEN}{LogUtils.BOLD}----- [Code Assembler] Execution succeeded! -----{LogUtils.RESET}")
        LogUtils.info("Captured Output", captured_output)
        
        # Persist only successful code to the RAG cache
        if state.get('tool_code_list'):
            # Save the current plan and the generated tool code
            rag_manager.save_new_tool(state['tool_plan'], state['tool_code_list'][0])
            
        return {
            "error": None, 
            "execution_result": captured_output, 
            "final_code": final_code
        }
# --------------------------------------------------
# 7. Graph Router
# --------------------------------------------------
def router(state: AgentState):
    if state['error']:
        if state['retry_count'] > MAX_RETRIES:
            print(f"{LogUtils.RED}   -> [Stop] Max retries reached.{LogUtils.RESET}")
            return "end"
        print(f"{LogUtils.YELLOW}   -> [Retry] Sending back to Supervisor.{LogUtils.RESET}")
        return "supervisor"
    return "end"

workflow = StateGraph(AgentState)
workflow.add_node("supervisor", supervisor_node)
workflow.add_node("tool_generator", tool_generator_node)
workflow.add_node("code_assembler", code_assembler_node)

workflow.set_entry_point("supervisor")
workflow.add_edge("supervisor", "tool_generator")
workflow.add_edge("tool_generator", "code_assembler")
workflow.add_conditional_edges("code_assembler", router, {"supervisor": "supervisor", "end": END})

app = workflow.compile()


MAX_RETRIES = 1


# --------------------------------------------------
# 8. Main Execution
# --------------------------------------------------
def main():
    if not os.path.exists(ProjectConfig.QUERY_FILE):
        print("Query file not found.")
        return

    df = load_query_file(ProjectConfig.QUERY_FILE)
    category_filter = get_env_setting("CATEGORY", "category").lower()
    query_no_min_raw = get_env_setting("QUERY_NO_MIN", "query_no_min")
    query_no_max_raw = get_env_setting("QUERY_NO_MAX", "query_no_max")
    query_no_min = None
    query_no_max = None
    if query_no_min_raw:
        try:
            query_no_min = int(float(query_no_min_raw))
        except Exception:
            print(f"Invalid QUERY_NO_MIN value: {query_no_min_raw}")
            return
    elif category_filter == "ocel":
        query_no_min = 60
    if query_no_max_raw:
        try:
            query_no_max = int(float(query_no_max_raw))
        except Exception:
            print(f"Invalid QUERY_NO_MAX value: {query_no_max_raw}")
            return
    elif category_filter == "general":
        query_no_max = 60

    if category_filter:
        print(f"[Run Filter] category={category_filter}")
    if query_no_min is not None:
        print(f"[Run Filter] query_no>={query_no_min}")
    if query_no_max is not None:
        print(f"[Run Filter] query_no<={query_no_max}")

    dataset_summaries = {
        "general": get_data_context("general"),
        "ocel": get_data_context("ocel"),
    }

    output_path = ProjectConfig.get_output_csv_path()
    
    for index, row in df.iterrows():
        user_query = get_query_text(row)
        if not user_query:
            continue
        query_no = get_query_no(row) or str(index + 1)
        query_no_value = parse_query_no_value(query_no)
        category = get_query_category(row)
        if category_filter and category != category_filter:
            continue
        if query_no_min is not None and (query_no_value is None or query_no_value < query_no_min):
            continue
        if query_no_max is not None and (query_no_value is None or query_no_value > query_no_max):
            continue
        benchmark_query_type = get_query_type(row)
        answer_format = get_answer_format(row)
        query_interpretation = get_query_interpretation(row)
        query_requirements = build_query_requirements(query_no, category, user_query, query_interpretation)
        rich_context = build_dataset_context(category, query_requirements=query_requirements)
        LogUtils.header(f"Query {query_no}: {user_query}")
        
        initial_state = {
            "query_no": query_no,
            "query": user_query,
            "category": category,
            "benchmark_query_type": benchmark_query_type,
            "answer_format": answer_format,
            "query_interpretation": query_interpretation,
            "query_requirements": query_requirements,
            "data_summary": dataset_summaries[category],
            "rich_context": rich_context,
            "tool_plan": "", 
            "analysis_plan": "", 
            "tool_code_list": [],
            "last_generated_tool_code": "",
            "last_failed_code": "",
            "final_code": "", 
            "execution_result": "", 
            "error": None, 
            "retry_count": 0
        }
        
        final_state = app.invoke(initial_state)
        
        if final_state.get('error'):
            final_ans = f"[FAILED] {final_state['error']}"
        else:
            final_ans = final_state.get('execution_result', 'Success but no output')
            
        df.at[index, 'Multi Agent'] = str(final_ans)
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"   {LogUtils.GREEN}✅ Progress Saved (Query {index+1}/{len(df)}){LogUtils.RESET}")

    print(f"\n{LogUtils.GREEN}All Completed. Final results saved to: {output_path}{LogUtils.RESET}")

if __name__ == "__main__":
    # [Added] Apply logging: redirect stdout/stderr to DualLogger
    sys.stdout = DualLogger(ProjectConfig.LOG_FILE)
    sys.stderr = sys.stdout # capture stderr in the log file as well
    
    main()
