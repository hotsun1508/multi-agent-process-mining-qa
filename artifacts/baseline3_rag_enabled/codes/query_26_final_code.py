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

    # Validate required columns
    required_cols = ['case:concept:name', 'concept:name', 'time:timestamp']
    missing = [c for c in required_cols if c not in log_df.columns]
    if missing:
        raise ValueError(
            'Missing required column(s) after dataframe conversion: ' + ', '.join(missing)
        )

    # Calculate transition durations
    log_df['duration'] = log_df.groupby('case:concept:name')['time:timestamp'].diff().dt.total_seconds()
    log_df = log_df.dropna(subset=['duration'])

    # Discover Directly-Follows Graph (DFG)
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)

    # Calculate average transition durations for each edge
    edge_durations = {}
    for i in range(len(log_df) - 1):
        src = log_df.iloc[i]['concept:name']
        dst = log_df.iloc[i + 1]['concept:name']
        if (src, dst) in edge_durations:
            edge_durations[(src, dst)].append(log_df.iloc[i + 1]['duration'])
        else:
            edge_durations[(src, dst)] = [log_df.iloc[i + 1]['duration']]

    # Calculate average durations
    avg_durations = {edge: sum(durations) / len(durations) for edge, durations in edge_durations.items()}

    # Identify the edge with the highest average duration
    slowest_edge = max(avg_durations, key=avg_durations.get)
    slowest_duration = avg_durations[slowest_edge]

    # Prepare final answer
    final_answer = {
        'slowest_edge': {'source': slowest_edge[0], 'target': slowest_edge[1], 'average_duration': slowest_duration},
        'result_type': 'composite',
        'view': 'event_log',
        'result_schema': {
            'process_discovery': 'dfg',
            'performance': 'metric_dict'
        },
        'artifacts_schema': ['output/*']
    }

    # Save DFG visualization
    dfg_path = os.path.join(output_dir, 'dfg_visualization.png')
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_path)
    print(f'OUTPUT_FILE_LOCATION: {dfg_path}')  

    # Save final answer to JSON
    answer_path = os.path.join(output_dir, 'final_answer.json')
    with open(answer_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {answer_path}')  

    print(json.dumps(final_answer, ensure_ascii=False))