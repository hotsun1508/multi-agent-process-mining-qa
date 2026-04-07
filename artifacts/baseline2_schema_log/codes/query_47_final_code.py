import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    # Convert event log to DataFrame
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])
    
    # Discover Directly-Follows Graph
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)
    
    # Find the most frequent edge
    most_frequent_edge = max(dfg.items(), key=lambda x: x[1])
    source, target = most_frequent_edge[0]
    
    # Filter cases containing the most frequent edge
    filtered_cases = log_df[(log_df['concept:name'].shift() == source) & (log_df['concept:name'] == target)]
    case_ids = filtered_cases['case:concept:name'].unique()
    filtered_log = log_df[log_df['case:concept:name'].isin(case_ids)]
    
    # Calculate average throughput time
    filtered_log['time:timestamp'] = pd.to_datetime(filtered_log['time:timestamp'])
    throughput_times = filtered_log.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
    throughput_times['throughput_time'] = (throughput_times['time:timestamp']['max'] - throughput_times['time:timestamp']['min']).dt.total_seconds() / 3600  # in hours
    average_throughput_time = throughput_times['throughput_time'].mean()
    
    # Identify top 5 resources
    top_resources = filtered_log['org:resource'].value_counts().head(5).to_dict()
    
    # Prepare final answer
    final_answer = {
        'most_frequent_edge': {'source': source, 'target': target},
        'average_throughput_time': average_throughput_time,
        'top_resources': top_resources
    }
    
    # Save results
    with open('output/results.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/results.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()