import json
import pandas as pd
import os


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for customers
    flattened_customers = pm4py.ocel_flattening(ocel, object_type='customers')
    # Convert to DataFrame and sort by timestamp
    log_df = pd.DataFrame(flattened_customers).sort_values(['case:concept:name', 'time:timestamp'])
    # Calculate throughput time for each case
    case_durations = log_df.groupby('case:concept:name').agg(
        throughput_time=('time:timestamp', lambda x: (x.max() - x.min()).total_seconds() / 3600)
    )
    # Calculate average and median throughput time
    average_throughput_time = case_durations['throughput_time'].mean()
    median_throughput_time = case_durations['throughput_time'].median()
    # Prepare the result
    throughput_stats = {
        'average_throughput_time_hours': average_throughput_time,
        'median_throughput_time_hours': median_throughput_time
    }
    # Save the result to JSON
    output_path = 'output/throughput_stats_customers.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(throughput_stats, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  
    # Prepare final answer for benchmark
    final_answer = {'performance': {'average': average_throughput_time, 'median': median_throughput_time}}
    print(json.dumps(final_answer, ensure_ascii=False))