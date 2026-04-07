import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])

    # Calculate throughput time for each case
    log_df['time:timestamp'] = pd.to_datetime(log_df['time:timestamp'])
    case_throughput = log_df.groupby('case:concept:name').agg(
        throughput_time=('time:timestamp', lambda x: (x.max() - x.min()).total_seconds())
    ).reset_index()

    # Identify the slowest 10% of cases
    threshold = case_throughput['throughput_time'].quantile(0.9)
    slow_cases = case_throughput[case_throughput['throughput_time'] >= threshold]['case:concept:name']

    # Filter the original log for slow cases
    slow_log_df = log_df[log_df['case:concept:name'].isin(slow_cases)]

    # Count the frequency of resources in the slow cases
    resource_counts = slow_log_df['org:resource'].value_counts().head(5)

    # Prepare the final answer
    final_answer = {'top_resources': resource_counts.to_dict()}

    # Save the results to a JSON file
    output_path = 'output/top_resources.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  

    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()