import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    # Calculate throughput time for each case
    log_df['throughput_time'] = log_df.groupby('case:concept:name')['time:timestamp'].transform(lambda x: (x.max() - x.min()).total_seconds())
    # Get the count of each variant
    variant_counts = log_df.groupby('case:concept:name')['concept:name'].value_counts().reset_index(name='count')
    # Filter variants that occur exactly once
    unique_variants = variant_counts[variant_counts['count'] == 1]['case:concept:name']
    # Filter the original log for these unique variants
    unique_cases = log_df[log_df['case:concept:name'].isin(unique_variants)]
    # Calculate the median throughput time
    median_throughput_time = unique_cases['throughput_time'].median()
    # Prepare the final answer
    final_answer = {'median_throughput_time': median_throughput_time}
    # Save the final answer to a JSON file
    with open('output/median_throughput_time.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/median_throughput_time.json')
    print(json.dumps(final_answer, ensure_ascii=False))

if __name__ == '__main__':
    main()