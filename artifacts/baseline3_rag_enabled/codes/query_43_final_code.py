import pm4py
import pandas as pd
import os
import json
from collections import defaultdict
import statistics

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
    log_df = log_df.dropna(subset=['duration'])

    # Discover Directly-Follows Graph (DFG)
    dfg = defaultdict(int)
    for i in range(len(log_df) - 1):
        if log_df.iloc[i]['case:concept:name'] == log_df.iloc[i + 1]['case:concept:name']:
            dfg[(log_df.iloc[i]['concept:name'], log_df.iloc[i + 1]['concept:name'])] += 1

    # Calculate average durations for each edge
    edge_durations = defaultdict(list)
    for i in range(len(log_df) - 1):
        if log_df.iloc[i]['case:concept:name'] == log_df.iloc[i + 1]['case:concept:name']:
            edge = (log_df.iloc[i]['concept:name'], log_df.iloc[i + 1]['concept:name'])
            edge_durations[edge].append(log_df.iloc[i + 1]['duration'])

    avg_durations = {edge: statistics.mean(durations) for edge, durations in edge_durations.items()}

    # Identify the slowest edge
    slowest_edge = max(avg_durations, key=avg_durations.get)
    slowest_avg_duration = avg_durations[slowest_edge]

    # Find top 5 resources involved in the slowest edge's activities
    involved_resources = log_df[(log_df['concept:name'] == slowest_edge[0]) | (log_df['concept:name'] == slowest_edge[1])]
    top_resources = involved_resources['org:resource'].value_counts().head(5).index.tolist()

    # Filter cases involving those resources and the slowest edge
    filtered_cases = log_df[log_df['org:resource'].isin(top_resources)]
    filtered_cases = filtered_cases[filtered_cases['concept:name'].isin(slowest_edge)]
    dominant_variant = filtered_cases['case:concept:name'].value_counts().idxmax()

    # Prepare final answer
    final_answer = {
        'dfg': dict(dfg),
        'highest_avg_duration_edge': {
            'edge': slowest_edge,
            'avg_duration': slowest_avg_duration,
            'top_resources': top_resources,
            'dominant_variant': dominant_variant
        }
    }

    # Save DFG visualization
    dfg_image_path = 'output/dfg_visualization.png'
    pm4py.save_vis_dfg(dfg, {}, {}, dfg_image_path)
    print(f'OUTPUT_FILE_LOCATION: {dfg_image_path}')  

    # Save final answer to JSON
    with open('output/benchmark_result.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/benchmark_result.json')

    print(json.dumps(final_answer, ensure_ascii=False))