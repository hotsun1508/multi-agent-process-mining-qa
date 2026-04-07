import pm4py
import pandas as pd
import json
import os
from collections import Counter


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Calculate case durations
    log_df["case_duration"] = log_df.groupby("case:concept:name")["time:timestamp"].transform(lambda x: (x.max() - x.min()).total_seconds())
    overall_avg_duration = log_df["case_duration"].mean()
    
    # Identify the strongest Working Together pair
    resource_pairs = log_df.groupby(["case:concept:name", "org:resource"])['concept:name'].count().reset_index()
    resource_pairs = resource_pairs[resource_pairs['concept:name'] > 1]
    pair_counts = resource_pairs.groupby("org:resource").size().reset_index(name='counts')
    strongest_pair = pair_counts.loc[pair_counts['counts'].idxmax()]
    
    # Select cases involving that pair
    filtered_cases = log_df[(log_df["org:resource"] == strongest_pair[0]) | (log_df["org:resource"] == strongest_pair[1])]
    
    # Isolate cases whose total case duration exceeds the overall average case duration
    delayed_cases = filtered_cases[filtered_cases["case_duration"] > overall_avg_duration]
    
    # Determine the dominant variant among those delayed cases
    variant_counts = delayed_cases.groupby(["case:concept:name", "concept:name"]).size().reset_index(name='counts')
    dominant_variant = variant_counts.groupby("case:concept:name")['counts'].sum().idxmax()
    
    # Filter for the dominant variant
    dominant_variant_cases = delayed_cases[delayed_cases["case:concept:name"] == dominant_variant]
    
    # Discover Directly-Follows Graph
    dfg, start_activities, end_activities = pm4py.discover_dfg(dominant_variant_cases)
    
    # Get top 5 edges
    total = sum(dfg.values()) if dfg else 0
    top_edges = [
        {"source": src, "target": dst, "count": count, "share": (count / total if total else 0.0)}
        for (src, dst), count in sorted(dfg.items(), key=lambda x: x[1], reverse=True)[:5]
    ]
    
    # Save DFG visualization
    dfg_png_path = "output/dfg_visualization.png"
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_png_path)
    print(f"OUTPUT_FILE_LOCATION: {dfg_png_path}")
    
    # Prepare final answer
    final_answer = {"top_edges": top_edges}
    print(json.dumps(final_answer, ensure_ascii=False))