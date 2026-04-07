import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Get the most frequent variants
    variant_counts = log_df.groupby(["case:concept:name"])['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    most_frequent_variants = variant_counts.head(10).index.tolist()
    
    # Filter the log for the most frequent variants
    filtered_log = log_df[log_df.groupby(["case:concept:name"])['concept:name'].apply(lambda x: ' -> '.join(x)).isin(most_frequent_variants)]
    
    # Create a DFG from the filtered log
    dfg, start_activities, end_activities = pm4py.discover_dfg(filtered_log)
    
    # Calculate average transition durations
    transition_durations = {}
    for (src, dst), count in dfg.items():
        durations = filtered_log[(filtered_log['concept:name'] == src) & (filtered_log['concept:name'].shift(-1) == dst)]["time:timestamp"].diff().dt.total_seconds().dropna()
        if not durations.empty:
            average_duration = durations.mean()
            transition_durations[(src, dst)] = average_duration
    
    # Identify the edge with the highest average transition duration
    if transition_durations:
        max_edge = max(transition_durations, key=transition_durations.get)
        max_duration = transition_durations[max_edge]
    else:
        max_edge = None
        max_duration = None
    
    # Save the DFG visualization
    dfg_png_path = "output/dfg_visualization.png"
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_png_path)
    print(f"OUTPUT_FILE_LOCATION: {dfg_png_path}")
    
    # Prepare final answer
    final_answer = {
        "most_frequent_variants": most_frequent_variants,
        "max_edge": max_edge,
        "max_duration": max_duration
    }
    
    # Save final answer to JSON
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))