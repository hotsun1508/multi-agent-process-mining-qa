import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    # Discover Directly-Follows Graph
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)
    
    # Save DFG visualization
    dfg_png_path = 'output/dfg_visualization.png'
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_png_path)
    print(f'OUTPUT_FILE_LOCATION: {dfg_png_path}')  
    
    # Identify the most frequent edge
    total = sum(dfg.values()) if dfg else 0
    most_frequent_edge = max(dfg.items(), key=lambda x: x[1])
    most_frequent_edge_activity = most_frequent_edge[0]
    
    # Filter cases containing the most frequent edge
    log_df = pm4py.convert_to_dataframe(event_log)
    filtered_cases = log_df[(log_df['concept:name'].shift() == most_frequent_edge_activity[0]) & (log_df['concept:name'] == most_frequent_edge_activity[1])]
    case_ids = filtered_cases['case:concept:name'].unique()
    filtered_log = log_df[log_df['case:concept:name'].isin(case_ids)]
    
    # Calculate average throughput time
    filtered_log['time:timestamp'] = pd.to_datetime(filtered_log['time:timestamp'])
    throughput_times = filtered_log.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
    throughput_times['throughput_time'] = (throughput_times['time:timestamp']['max'] - throughput_times['time:timestamp']['min']).dt.total_seconds() / 3600  # in hours
    average_throughput_time = throughput_times['throughput_time'].mean()
    
    # Identify top 5 resources in the filtered cases
    top_resources = filtered_log['org:resource'].value_counts().head(5).to_dict()
    
    # Prepare final answer
    final_answer = {
        'process_discovery': {
            'most_frequent_edge': most_frequent_edge_activity,
            'average_throughput_time': average_throughput_time
        },
        'resource': top_resources
    }
    
    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))