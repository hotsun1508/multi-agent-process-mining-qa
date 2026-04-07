import pm4py
import pandas as pd
import json
import os


def compute_top_variant_coverage(event_log, top_percent=20, coverage_threshold=0.8):
    if event_log is None:
        raise ValueError("event_log is None. Expected a PM4Py EventLog.")
    if not hasattr(event_log, "__iter__"):
        raise TypeError("Invalid event_log type. Expected an iterable PM4Py EventLog.")

    df = pm4py.convert_to_dataframe(event_log)
    variants_raw = pm4py.get_variants(event_log)
    total_cases = len(df["case:concept:name"].unique())
    variant_counts = sorted(variants_raw.values(), reverse=True)
    top_n = int(len(variant_counts) * (top_percent / 100))
    top_variants_count = sum(variant_counts[:top_n])
    coverage = top_variants_count / total_cases
    return coverage >= coverage_threshold


def main():
    event_log = ACTIVE_LOG
    result = compute_top_variant_coverage(event_log)
    final_answer = {"top_20_percent_cover_80_percent_cases": result}
    os.makedirs("output", exist_ok=True)
    with open("output/benchmark_result.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: output/benchmark_result.json")
    print(json.dumps(final_answer, ensure_ascii=False))