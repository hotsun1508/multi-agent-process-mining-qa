import pm4py
import json
import pandas as pd
import numpy as np


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for customers
    flattened_customers = pm4py.ocel_flattening(ocel, 'customers')
    # Convert to DataFrame
    log_df = pm4py.convert_to_dataframe(flattened_customers)
    # Sort by case and timestamp
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])
    # Calculate throughput time for each case
    log_df['throughput_time'] = log_df.groupby('case:concept:name')['time:timestamp'].transform(lambda x: (x.max() - x.min()).total_seconds() / 3600)
    # Compute average and median throughput time
    average_throughput_time = log_df['throughput_time'].mean()
    median_throughput_time = log_df['throughput_time'].median()
    # Prepare the result
    throughput_stats = {
        'average_throughput_time_hours': average_throughput_time,
        'median_throughput_time_hours': median_throughput_time
    }
    # Save the result to JSON
    with open('output/throughput_stats_customers.json', 'w', encoding='utf-8') as f:
        json.dump(throughput_stats, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/throughput_stats_customers.json')
    # Prepare final answer
    final_answer = {'performance': {'average_throughput_time_hours': average_throughput_time, 'median_throughput_time_hours': median_throughput_time}}
    print(json.dumps(final_answer, ensure_ascii=False))