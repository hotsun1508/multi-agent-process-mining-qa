import pm4py
import pandas as pd
import json
import os
from collections import defaultdict


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    # Calculate transition durations
    log_df["duration"] = log_df.groupby("case:concept:name")["time:timestamp"].diff().dt.total_seconds()
    # Discover Directly-Follows Graph
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)
    # Calculate average duration for each edge
    edge_durations = defaultdict(list)
    for i in range(len(log_df) - 1):
        current_activity = log_df.iloc[i]["concept:name"]
        next_activity = log_df.iloc[i + 1]["concept:name"]
        if current_activity in dfg and next_activity in dfg[current_activity]:
            edge_durations[(current_activity, next_activity)].append(log_df.iloc[i + 1]["duration"])
    average_durations = {edge: sum(durations) / len(durations) for edge, durations in edge_durations.items() if durations}
    # Identify the slowest edge
    slowest_edge = max(average_durations, key=average_durations.get)
    slowest_duration = average_durations[slowest_edge]
    # Get resources involved in the source and target activities
    source_activity, target_activity = slowest_edge
    involved_resources = log_df[(log_df["concept:name"] == source_activity) | (log_df["concept:name"] == target_activity)]["org:resource"].value_counts().head(5).index.tolist()
    # Save DFG visualization
    png_path = "output/dfg_visualization.png"
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, png_path)
    print(f"OUTPUT_FILE_LOCATION: {png_path}")
    # Save average durations
    with open("output/average_durations.json", "w", encoding="utf-8") as f:
        json.dump(average_durations, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/average_durations.json")
    # Prepare final answer
    final_answer = {
        "slowest_edge": {
            "edge": slowest_edge,
            "average_duration": slowest_duration,
            "top_resources": involved_resources
        }
    }
    print(json.dumps(final_answer, ensure_ascii=False))