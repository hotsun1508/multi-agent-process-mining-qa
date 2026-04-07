import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)
    transition_durations = log_df.groupby(["case:concept:name", "concept:name"])['time:timestamp'].diff().dt.total_seconds().fillna(0)
    log_df["duration"] = transition_durations
    dfg_performance = {edge: [] for edge in dfg.keys()}
    for case_id, group in log_df.groupby("case:concept:name"):
        for i in range(len(group) - 1):
            edge = (group.iloc[i]["concept:name"], group.iloc[i + 1]["concept:name"])
            dfg_performance[edge].append(group.iloc[i + 1]["duration"])
    average_durations = {edge: sum(durations) / len(durations) if durations else 0 for edge, durations in dfg_performance.items()}
    max_edge = max(average_durations, key=average_durations.get)
    max_edge_avg_duration = average_durations[max_edge]
    cases_with_max_edge = log_df[(log_df["concept:name"].shift() == max_edge[0]) & (log_df["concept:name"] == max_edge[1])]
    cases_with_max_edge_count = len(cases_with_max_edge["case:concept:name"].unique())
    variants = log_df.groupby(["case:concept:name"])['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    top_20_percent_threshold = int(len(variants) * 0.2)
    top_20_percent_variants = variants.nlargest(top_20_percent_threshold).index
    cases_in_top_variants = cases_with_max_edge[cases_with_max_edge["case:concept:name"].isin(top_20_percent_variants)]
    percentage_top_variants = (len(cases_in_top_variants["case:concept:name"].unique()) / cases_with_max_edge_count) * 100 if cases_with_max_edge_count > 0 else 0
    final_answer = {"max_edge": max_edge, "max_edge_avg_duration": max_edge_avg_duration, "percentage_top_variants": percentage_top_variants}
    with open("output/benchmark_result.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: output/benchmark_result.json")
    print(json.dumps(final_answer, ensure_ascii=False))