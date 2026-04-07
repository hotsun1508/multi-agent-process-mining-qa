import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Calculate sojourn time for each activity
    log_df['time:timestamp'] = pd.to_datetime(log_df['time:timestamp'])
    log_df['next_timestamp'] = log_df.groupby('case:concept:name')['time:timestamp'].shift(-1)
    log_df['sojourn_time'] = (log_df['next_timestamp'] - log_df['time:timestamp']).dt.total_seconds()
    
    # Calculate average sojourn time per activity
    avg_sojourn_time = log_df.groupby('concept:name')['sojourn_time'].mean().sort_values(ascending=False)
    longest_activity = avg_sojourn_time.idxmax()
    
    # Get top 5 resources executing the longest activity
    top_resources = log_df[log_df['concept:name'] == longest_activity]['org:resource'].value_counts().head(5).index.tolist()
    
    # Filter log for cases involving those resources
    filtered_cases = log_df[log_df['org:resource'].isin(top_resources)]
    
    # Determine the dominant variant among the filtered cases
    variants = filtered_cases.groupby('case:concept:name')['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    dominant_variant = variants.idxmax()
    
    # Filter log for the dominant variant
    dominant_cases = filtered_cases[filtered_cases['case:concept:name'].isin(variants[variants.index == dominant_variant].index)]
    
    # Discover Directly-Follows Graph (DFG)
    dfg, start_activities, end_activities = pm4py.discover_dfg(dominant_cases)
    
    # Get top 5 edges from the DFG
    total = sum(dfg.values()) if dfg else 0
    top_edges = [
        {"source": src, "target": dst, "count": count, "share": (count / total if total else 0.0)}
        for (src, dst), count in sorted(dfg.items(), key=lambda x: x[1], reverse=True)[:5]
    ]
    
    # Prepare final answer
    final_answer = {
        "longest_activity": longest_activity,
        "top_resources": top_resources,
        "dominant_variant": dominant_variant,
        "top_edges": top_edges
    }
    
    # Save DFG visualization
    png_path = "output/dfg_visualization.png"
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, png_path)
    print(f"OUTPUT_FILE_LOCATION: {png_path}")
    
    # Save final answer to JSON
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))