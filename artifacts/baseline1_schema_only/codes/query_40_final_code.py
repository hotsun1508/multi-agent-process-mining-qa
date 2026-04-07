import pm4py
import pandas as pd
import json
import os
from collections import defaultdict


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Discover Directly-Follows Graph (DFG)
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)
    
    # Calculate transition durations
    transition_durations = defaultdict(list)
    for case_id, group in log_df.groupby("case:concept:name"):
        timestamps = group["time:timestamp"].tolist()
        activities = group["concept:name"].tolist()
        for i in range(len(activities) - 1):
            transition = (activities[i], activities[i + 1])
            duration = (timestamps[i + 1] - timestamps[i]).total_seconds()
            transition_durations[transition].append(duration)
    
    # Calculate average durations and find the edge with the highest average duration
    avg_durations = {edge: sum(durations) / len(durations) for edge, durations in transition_durations.items() if durations}
    max_edge = max(avg_durations, key=avg_durations.get)
    max_avg_duration = avg_durations[max_edge]
    
    # Identify cases containing the edge with the highest average duration
    cases_with_max_edge = log_df[(log_df["concept:name"] == max_edge[0]) | (log_df["concept:name"] == max_edge[1])]["case:concept:name"].unique()
    
    # Calculate the top 20% most frequent variants
    variant_counts = log_df.groupby(["case:concept:name", "concept:name"]).size().reset_index(name='counts')
    top_20_percent_threshold = variant_counts["counts"].quantile(0.8)
    top_variants = variant_counts[variant_counts["counts"] >= top_20_percent_threshold]["case:concept:name"].unique()
    
    # Calculate the percentage of cases with the max edge that belong to the top 20% variants
    cases_with_max_edge_count = len(cases_with_max_edge)
    cases_in_top_variants_count = len(set(cases_with_max_edge) & set(top_variants))
    percentage_in_top_variants = (cases_in_top_variants_count / cases_with_max_edge_count) * 100 if cases_with_max_edge_count > 0 else 0
    
    # Save the DFG visualization
    png_path = "output/dfg_visualization.png"
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, png_path)
    print(f"OUTPUT_FILE_LOCATION: {png_path}")
    
    # Prepare final answer
    final_answer = {
        "max_edge": max_edge,
        "max_avg_duration": max_avg_duration,
        "percentage_in_top_variants": percentage_in_top_variants
    }
    
    # Save final answer to JSON
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))