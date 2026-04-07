import pm4py
import pandas as pd
import json
import os
import statistics
from collections import Counter


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)
    transition_durations = log_df.groupby(["concept:name", "case:concept:name"])["time:timestamp"].diff().dt.total_seconds().dropna()
    transition_durations = transition_durations.reset_index(drop=True)
    dfg_durations = {edge: [] for edge in dfg.keys()}
    for (src, tgt), count in dfg.items():
        durations = transition_durations[(log_df["concept:name"].shift() == src) & (log_df["concept:name"] == tgt)]
        dfg_durations[(src, tgt)] = durations.tolist()
    avg_durations = {edge: statistics.mean(durations) for edge, durations in dfg_durations.items() if durations}
    slowest_edge = max(avg_durations, key=avg_durations.get)
    slowest_edge_duration = avg_durations[slowest_edge]
    top_resources = log_df[(log_df["concept:name"].isin(slowest_edge))]["org:resource"].value_counts().head(5).index.tolist()
    filtered_cases = log_df[log_df["org:resource"].isin(top_resources)]
    case_variants = filtered_cases.groupby("case:concept:name")["concept:name"].apply(lambda x: " -> ".join(x)).value_counts()
    dominant_variant = case_variants.idxmax()
    png_path = "output/dfg_visualization.png"
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, png_path)
    print(f"OUTPUT_FILE_LOCATION: {png_path}")
    final_answer = {
        "slowest_edge": slowest_edge,
        "slowest_edge_duration": slowest_edge_duration,
        "top_resources": top_resources,
        "dominant_variant": dominant_variant
    }
    with open("output/benchmark_result.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: output/benchmark_result.json")
    print(json.dumps(final_answer, ensure_ascii=False))