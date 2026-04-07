import pm4py
import pandas as pd
import json
import os
from collections import Counter


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])

    # Calculate sojourn time for each activity
    log_df['sojourn_time'] = log_df.groupby(['case:concept:name', 'concept:name'])['time:timestamp'].transform(lambda x: x.max() - x.min()).dt.total_seconds()
    avg_sojourn_time = log_df.groupby('concept:name')['sojourn_time'].mean().sort_values(ascending=False)
    longest_activity = avg_sojourn_time.idxmax()
    longest_activity_avg_time = avg_sojourn_time.max()

    # Get top 5 resources executing the longest activity
    top_resources = log_df[log_df['concept:name'] == longest_activity]['org:resource'].value_counts().head(5).index.tolist()

    # Filter log for cases involving those resources
    filtered_cases = log_df[log_df['org:resource'].isin(top_resources)]['case:concept:name'].unique()
    filtered_log_df = log_df[log_df['case:concept:name'].isin(filtered_cases)]

    # Determine the dominant variant
    variant_counts = filtered_log_df.groupby(['case:concept:name', 'concept:name']).size().reset_index(name='count')
    dominant_variant = variant_counts.groupby('case:concept:name')['count'].sum().idxmax()

    # Discover Directly-Follows Graph from the dominant variant subset
    dfg, start_activities, end_activities = pm4py.discover_dfg(filtered_log_df)
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
    final_answer = {
        "longest_activity": longest_activity,
        "longest_activity_avg_time": longest_activity_avg_time,
        "top_resources": top_resources,
        "dominant_variant": dominant_variant,
        "top_edges": top_edges
    }

    print(json.dumps(final_answer, ensure_ascii=False))