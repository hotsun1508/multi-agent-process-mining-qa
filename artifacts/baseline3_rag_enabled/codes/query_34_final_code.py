import os
import json
import pm4py
import pandas as pd


def main():
    event_log = ACTIVE_LOG
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Convert to DataFrame
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])

    # Get the most frequent variants
    variants = pm4py.get_variants(event_log)
    most_frequent_variant = max(variants.items(), key=lambda x: x[1])[0]
    filtered_cases = log_df[log_df['case:concept:name'].isin([case for case in variants if case == most_frequent_variant])]

    # Generate Directly-Follows Graph (DFG)
    dfg, start_activities, end_activities = pm4py.discover_dfg(filtered_cases)
    dfg_path = os.path.join(output_dir, 'dfg_visualization.png')
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_path)
    print(f'OUTPUT_FILE_LOCATION: {dfg_path}')  

    # Calculate average transition durations
    transition_durations = {}
    for case_id, group in filtered_cases.groupby('case:concept:name'):
        timestamps = group['time:timestamp'].tolist()
        activities = group['concept:name'].tolist()
        for i in range(len(activities) - 1):
            edge = (activities[i], activities[i + 1])
            duration = (timestamps[i + 1] - timestamps[i]).total_seconds()
            if edge not in transition_durations:
                transition_durations[edge] = []
            transition_durations[edge].append(duration)

    # Identify the edge with the highest average transition duration
    avg_durations = {edge: sum(durations) / len(durations) for edge, durations in transition_durations.items()}
    slowest_edge = max(avg_durations.items(), key=lambda x: x[1])

    # Prepare final answer
    final_answer = {
        'slowest_edge': slowest_edge[0],
        'average_duration': slowest_edge[1]
    }
    with open(os.path.join(output_dir, 'benchmark_result.json'), 'w') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'benchmark_result.json')}')

    print(json.dumps(final_answer, ensure_ascii=False))