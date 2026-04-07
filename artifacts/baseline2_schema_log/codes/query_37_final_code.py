import pm4py
import pandas as pd
import json
import os
from collections import Counter

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Get the variants occurring exactly once
    variants = log_df.groupby("case:concept:name")["concept:name"].apply(lambda x: tuple(x)).value_counts()
    single_variants = variants[variants == 1].index.tolist()
    single_case_ids = log_df[log_df["case:concept:name"].isin(single_variants)]["case:concept:name"].unique()
    
    # Filter the log for cases with single variants
    filtered_log_df = log_df[log_df["case:concept:name"].isin(single_case_ids)]
    
    # Discover Directly-Follows Graph
    dfg, start_activities, end_activities = pm4py.discover_dfg(filtered_log_df)
    
    # Save DFG visualization
    dfg_png_path = "output/dfg_visualization.png"
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_png_path)
    print(f"OUTPUT_FILE_LOCATION: {dfg_png_path}")
    
    # Count occurrences of edges and find the most frequent edge
    edge_counter = Counter(dfg)
    most_frequent_edge = edge_counter.most_common(1)[0]
    most_frequent_edge_activities = most_frequent_edge[0]
    
    # Get resources for the most frequent edge
    edge_source, edge_target = most_frequent_edge_activities
    resources_on_edge = filtered_log_df[(filtered_log_df["concept:name"] == edge_source) | (filtered_log_df["concept:name"] == edge_target)]["org:resource"]
    resource_counts = resources_on_edge.value_counts()
    top_resources = resource_counts.head(5).to_dict()
    
    # Prepare final answer
    final_answer = {
        "most_frequent_edge": {
            "source": edge_source,
            "target": edge_target,
            "count": most_frequent_edge[1],
            "top_resources": top_resources
        }
    }
    
    # Save final answer to JSON
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))