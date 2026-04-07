import pm4py
import pandas as pd
import json
import os
from collections import Counter


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Get the variants occurring exactly once
    variant_counts = log_df.groupby("case:concept:name")["concept:name"].apply(lambda x: tuple(x)).value_counts()
    unique_variants = variant_counts[variant_counts == 1].index.tolist()
    unique_cases = log_df[log_df.groupby("case:concept:name")["concept:name"].transform(lambda x: tuple(x) in unique_variants)]
    
    # Discover Directly-Follows Graph
    dfg, start_activities, end_activities = pm4py.discover_dfg(unique_cases)
    
    # Save DFG visualization
    dfg_png_path = "output/dfg_visualization.png"
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_png_path)
    print(f"OUTPUT_FILE_LOCATION: {dfg_png_path}")
    
    # Find the most frequent edge
    edge_counts = Counter(dfg)
    most_frequent_edge = edge_counts.most_common(1)[0]
    source, target = most_frequent_edge[0]
    count = most_frequent_edge[1]
    
    # Get resources for the most frequent edge
    resources_on_edge = unique_cases[(unique_cases["concept:name"] == source) | (unique_cases["concept:name"] == target)]["org:resource"]
    resource_counts = Counter(resources_on_edge)
    top_resources = resource_counts.most_common(5)
    
    # Prepare final answer
    final_answer = {
        "most_frequent_edge": {"source": source, "target": target, "count": count},
        "top_resources": top_resources
    }
    
    # Save final answer to JSON
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))