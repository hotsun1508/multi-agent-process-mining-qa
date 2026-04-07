import pm4py
import pandas as pd
import json
import os
import statistics


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Get the most frequent variants
    variant_counts = log_df.groupby(["case:concept:name"])['concept:name'].count().reset_index(name='count')
    most_frequent_variants = variant_counts.nlargest(10, 'count')['case:concept:name'].tolist()
    filtered_log_df = log_df[log_df['case:concept:name'].isin(most_frequent_variants)]
    
    # Generate Directly-Follows Graph (DFG)
    dfg, start_activities, end_activities = pm4py.discover_dfg(filtered_log_df)
    
    # Calculate average transition durations
    transition_durations = {}
    for i in range(len(filtered_log_df) - 1):
        current_activity = filtered_log_df.iloc[i]['concept:name']
        next_activity = filtered_log_df.iloc[i + 1]['concept:name']
        duration = (filtered_log_df.iloc[i + 1]['time:timestamp'] - filtered_log_df.iloc[i]['time:timestamp']).total_seconds()
        if (current_activity, next_activity) in transition_durations:
            transition_durations[(current_activity, next_activity)].append(duration)
        else:
            transition_durations[(current_activity, next_activity)] = [duration]
    
    # Find the edge with the highest average duration
    max_avg_duration = 0
    max_edge = None
    for edge, durations in transition_durations.items():
        avg_duration = statistics.mean(durations)
        if avg_duration > max_avg_duration:
            max_avg_duration = avg_duration
            max_edge = edge
    
    # Save DFG visualization
    png_path = "output/dfg_visualization.png"
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, png_path)
    print(f"OUTPUT_FILE_LOCATION: {png_path}")
    
    # Save DFG data
    with open("output/dfg.pkl", "wb") as f:
        pickle.dump(dfg, f)
    print("OUTPUT_FILE_LOCATION: output/dfg.pkl")
    
    # Prepare final answer
    final_answer = {
        "most_frequent_variants": most_frequent_variants,
        "max_edge": max_edge,
        "max_avg_duration": max_avg_duration
    }
    print(json.dumps(final_answer, ensure_ascii=False))