import os
import json
import pm4py
import pandas as pd


def main():
    event_log = ACTIVE_LOG
    # Convert event log to DataFrame
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    # Get the most frequent variant
    variant_counts = log_df.groupby(['case:concept:name'])['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    most_frequent_variant = variant_counts.idxmax()
    
    # Filter cases that follow the most frequent variant
    filtered_cases = log_df[log_df.groupby('case:concept:name')['concept:name'].transform(lambda x: ' -> '.join(x)) == most_frequent_variant]
    
    # Calculate throughput time for each case
    throughput_times = filtered_cases.groupby('case:concept:name').agg(
        throughput_time=('time:timestamp', lambda x: (x.max() - x.min()).total_seconds())
    )
    
    # Calculate average throughput time
    average_throughput_time = throughput_times['throughput_time'].mean()
    
    # Prepare final answer
    final_answer = {'average_throughput_time': average_throughput_time}
    
    # Save final answer to JSON
    with open('output/average_throughput_time.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/average_throughput_time.json')
    
    # Print final answer
    print(json.dumps(final_answer, ensure_ascii=False))