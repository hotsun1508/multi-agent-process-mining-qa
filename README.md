# Multi-Agent Process Mining QA

Code and packaged experiment assets for the paper:

`ASPAI26: Multi-Agent Orchestration Framework for Complex Process Mining Query Answering`

This repository is a cleaned public-release package extracted from the original research workspace. It includes:

- The final multi-agent implementation for OCEL query answering
- Three single-agent baseline variants
- Query files, schemas, precomputed process abstractions, RAG resources, and selected final experiment artifacts
- The paper PDF used for the release package

## Included Code

- `src/multi_agent_framework.py`
- `src/baselines/baseline1_schema_only.py`
- `src/baselines/baseline2_schema_log.py`
- `src/baselines/baseline3_rag_enabled.py`
- `src/baselines/baseline_common.py`

## Repository Structure

```text
.
├── artifacts/                      # Selected final experiment outputs
├── context/                        # Precomputed process abstraction summaries
├── data/                           # Input event logs, schemas, OCEL data
├── paper/                          # Paper PDF
├── query/                          # Evaluation query files
├── resources/pm4py_faiss_db/       # PM4Py manual/tool RAG database
└── src/                            # Main runnable implementations
```

## Environment

- Python 3.10 or newer is recommended
- OpenAI API key is required
- This repository contains large files; Git LFS is recommended before pushing to GitHub

Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create `.env` from `.env.example`:

```bash
cp .env.example .env
```

## Large File Setup

This package includes files larger than GitHub's normal file-size limit, including:

- `data/BPI Challenge 2017.xes`
- large result JSON/CSV files under `artifacts/`

Before pushing to GitHub:

```bash
git lfs install
git add .gitattributes
```

If you prefer a lighter repository, move `data/` and `artifacts/` to Zenodo, Hugging Face, or GitHub Releases and keep links here instead.

## Running the Main Methods

Run from the repository root.

Multi-agent:

```bash
python src/multi_agent_framework.py
```

Single-agent baseline 1:

```bash
python src/baselines/baseline1_schema_only.py
```

Single-agent baseline 2:

```bash
python src/baselines/baseline2_schema_log.py
```

Single-agent baseline 3:

```bash
python src/baselines/baseline3_rag_enabled.py
```

Optional environment variable for partial benchmark execution:

```bash
RUN_QUERY_NOS=61,62 python src/multi_agent_framework.py
```

## Outputs

New runs generate timestamped outputs under `artifacts/`.

The packaged release already includes selected final result directories:

- `artifacts/multi_agent_framework`
- `artifacts/baseline1_schema_only`
- `artifacts/baseline2_schema_log`
- `artifacts/baseline3_rag_enabled`

Note: the bundled multi-agent artifact is the latest available executed run from the `v1-8` series, while the packaged source code is the cleaned final code snapshot.

## Notes

- `.env` is intentionally excluded from version control
- The code was adjusted to use repository-relative paths for public release
- Public script names were simplified for readability
