import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])

    # Step 1: Identify the top 5 resources by event frequency
    top_resources = log_df['org:resource'].value_counts().head(5).index.tolist()

    # Step 2: Filter cases involving at least one of the top resources
    filtered_cases = log_df[log_df['org:resource'].isin(top_resources)]
    case_durations = filtered_cases.groupby('case:concept:name').agg(duration=('time:timestamp', 'max')).reset_index()
    case_durations['duration'] = case_durations['duration'] - filtered_cases.groupby('case:concept:name')['time:timestamp'].min().values

    # Step 3: Calculate overall average case duration
    overall_average_duration = case_durations['duration'].mean()

    # Step 4: Find cases whose total case duration exceeds the overall average
    delayed_cases = case_durations[case_durations['duration'] > overall_average_duration]
    delayed_case_ids = delayed_cases['case:concept:name'].tolist()

    # Step 5: Filter the original log for delayed cases
    delayed_cases_log = log_df[log_df['case:concept:name'].isin(delayed_case_ids)]

    # Step 6: Identify the dominant variant among those delayed cases
    variant_counts = delayed_cases_log.groupby(['case:concept:name', 'concept:name']).size().reset_index(name='count')
    dominant_variant = variant_counts.groupby('case:concept:name')['count'].idxmax()
    dominant_variants = variant_counts.loc[dominant_variant]

    # Prepare the final result dictionary
    result = {
        'primary_answer_in_csv_log': True,
        'result_type': 'composite',
        'view': 'event_log',
        'result_schema': {
            'performance': {
                'delayed_case_ids': delayed_case_ids,
                'overall_average_duration': overall_average_duration
            },
            'behavior_variant': {
                'dominant_variant': dominant_variants['concept:name'].tolist()
            }
        },
        'artifacts_schema': ['output/* (optional auxiliary artifacts such as png/csv/pkl/json)']
    }

    # Save the result as JSON
    with open('output/result.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/result.json')

    # Print the final answer
    print(json.dumps(result, ensure_ascii=False))