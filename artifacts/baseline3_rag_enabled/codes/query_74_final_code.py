import os
import pandas as pd
import json


def main():
    ocel = ACTIVE_LOG
    # Step 1: Flatten the OCEL for the object type 'customers'
    flattened_customers = pm4py.ocel_flattening(ocel, 'customers')
    
    # Step 2: Calculate throughput time for each case
    flattened_customers = flattened_customers.sort_values(by=['case:concept:name', 'time:timestamp'])
    throughput_times = []
    for case_id, group in flattened_customers.groupby('case:concept:name'):
        start_time = group['time:timestamp'].min()
        end_time = group['time:timestamp'].max()
        throughput_time_hours = (end_time - start_time).total_seconds() / 3600.0
        throughput_times.append(throughput_time_hours)
    
    # Step 3: Calculate average and median throughput times
    average_throughput_time = sum(throughput_times) / len(throughput_times)
    median_throughput_time = sorted(throughput_times)[len(throughput_times) // 2]
    
    # Step 4: Save the computed statistics as a JSON file
    throughput_stats = {
        'average_throughput_time_hours': average_throughput_time,
        'median_throughput_time_hours': median_throughput_time
    }
    os.makedirs('output', exist_ok=True)
    with open('output/throughput_stats_customers.json', 'w') as json_file:
        json.dump(throughput_stats, json_file, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/throughput_stats_customers.json')
    
    # Step 5: Prepare the final benchmark answer
    final_answer = {
        'performance': {
            'average_throughput_time_hours': average_throughput_time,
            'median_throughput_time_hours': median_throughput_time
        }
    }
    print(json.dumps(final_answer, ensure_ascii=False))