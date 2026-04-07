import pandas as pd
import json
import os
from pm4py.objects.log.util import dataframe_utils


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])

    # Calculate throughput time for each case
    log_df['throughput_time'] = log_df.groupby('case:concept:name')['time:timestamp'].transform(lambda x: (x.max() - x.min()).total_seconds())

    # Identify the slowest 10% of cases
    threshold = log_df['throughput_time'].quantile(0.9)
    slow_cases = log_df[log_df['throughput_time'] >= threshold]

    # Count the frequency of resources in the slow cases
    resource_counts = slow_cases['org:resource'].value_counts().head(5)

    # Prepare the final answer
    final_answer = {'top_resources': resource_counts.to_dict()}

    # Save the final answer to a JSON file
    output_path = 'output/slowest_cases_resources.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  

    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()