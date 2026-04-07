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

    # Calculate transition durations
    log_df['duration'] = log_df.groupby('case:concept:name')['time:timestamp'].diff().dt.total_seconds()
    log_df['prev_activity'] = log_df.groupby('case:concept:name')['concept:name'].shift(1)

    # Filter out the first event of each case
    filtered_df = log_df[~log_df['prev_activity'].isnull()]

    # Calculate average duration for each edge
    avg_durations = filtered_df.groupby(['prev_activity', 'concept:name'])['duration'].mean().reset_index()
    avg_durations.columns = ['source', 'target', 'avg_duration']

    # Identify the slowest edge
    slowest_edge = avg_durations.loc[avg_durations['avg_duration'].idxmax()]
    source_activity = slowest_edge['source']
    target_activity = slowest_edge['target']
    avg_duration = slowest_edge['avg_duration']

    # Get resources involved in the source and target activities
    involved_resources = log_df[(log_df['concept:name'] == source_activity) | (log_df['concept:name'] == target_activity)]
    top_resources = involved_resources['org:resource'].value_counts().head(5).index.tolist()

    # Prepare final answer
    final_answer = {
        'dfg': avg_durations.to_dict(orient='records'),
        'highest_avg_duration_edge': {
            'edge': (source_activity, target_activity),
            'avg_duration': avg_duration,
            'top_resources': top_resources
        }
    }

    # Save DFG visualization
    dfg = avg_durations.set_index(['source', 'target'])['avg_duration'].to_dict()
    pm4py.save_vis_dfg(dfg, output_dir + '/dfg_visualization.png')
    print(f'OUTPUT_FILE_LOCATION: {output_dir}/dfg_visualization.png')

    # Save final answer to JSON
    with open(output_dir + '/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {output_dir}/final_answer.json')

    print(json.dumps(final_answer, ensure_ascii=False))