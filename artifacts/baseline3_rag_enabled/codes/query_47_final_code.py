import pm4py
import pandas as pd
import os
import json


def main():
    event_log = ACTIVE_LOG
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Convert to DataFrame
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])

    # Discover Directly-Follows Graph (DFG)
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)

    # Identify the most frequent edge
    most_frequent_edge = max(dfg.items(), key=lambda x: x[1])
    source_activity, target_activity = most_frequent_edge[0]

    # Filter cases containing the most frequent edge
    filtered_cases = log_df[(log_df['concept:name'] == source_activity) | (log_df['concept:name'] == target_activity)]
    case_ids = filtered_cases['case:concept:name'].unique()
    filtered_cases = log_df[log_df['case:concept:name'].isin(case_ids)]

    # Calculate average throughput time for filtered cases
    case_durations = filtered_cases.groupby('case:concept:name').agg(
        start_time=('time:timestamp', 'min'),
        end_time=('time:timestamp', 'max')
    )
    case_durations['throughput_time'] = (case_durations['end_time'] - case_durations['start_time']).dt.total_seconds()
    average_throughput_time = case_durations['throughput_time'].mean()

    # Identify top 5 resources in filtered cases
    top_resources = filtered_cases['org:resource'].value_counts().head(5).to_dict()

    # Prepare final result
    final_answer = {
        'primary_answer_in_csv_log': True,
        'result_type': 'composite',
        'view': 'event_log',
        'result_schema': {
            'process_discovery': 'dfg',
            'performance': {'average_throughput_time': average_throughput_time},
            'resource': top_resources,
            'behavior_variant': 'N/A'
        }
    }

    # Save the DFG visualization
    dfg_path = 'output/dfg_visualization.png'
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_path)
    print(f'OUTPUT_FILE_LOCATION: {dfg_path}')  

    # Save the final answer as JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')

    print(json.dumps(final_answer, ensure_ascii=False))