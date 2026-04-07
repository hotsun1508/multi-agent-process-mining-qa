import pm4py
import pandas as pd
import os
import json


def analyze_slowest_cases_resources(event_log):
    # Convert the event log to a DataFrame
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])

    # Calculate case durations
    case_times = log_df.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_times['duration'] = (case_times['max'] - case_times['min']).dt.total_seconds()

    # Determine the threshold for the slowest 10% of cases
    threshold = case_times['duration'].quantile(0.9)
    slowest_cases = case_times[case_times['duration'] > threshold].index.tolist()

    # Filter the log for slowest cases
    slowest_log_df = log_df[log_df['case:concept:name'].isin(slowest_cases)]

    # Identify the top 5 resources by frequency in the slowest cases
    top_resources = slowest_log_df['org:resource'].value_counts().head(5).reset_index()
    top_resources.columns = ['resource_id', 'frequency']

    # Prepare the final result dictionary
    result = {
        'primary_answer_in_csv_log': True,
        'result_type': 'composite',
        'view': 'event_log',
        'result_schema': {
            'performance': {
                'slowest_case_ids': slowest_cases
            },
            'resource': top_resources.to_dict(orient='records')
        },
        'artifacts_schema': ['output/* (optional auxiliary artifacts such as png/csv/pkl/json)']
    }

    # Save the result to a JSON file
    output_path = 'output/slowest_cases_resources.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  

    return result


def main():
    event_log = ACTIVE_LOG
    final_answer = analyze_slowest_cases_resources(event_log)
    print(json.dumps(final_answer, ensure_ascii=False))