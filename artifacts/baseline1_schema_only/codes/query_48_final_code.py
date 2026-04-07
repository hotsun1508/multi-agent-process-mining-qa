import pm4py
import pandas as pd
import json
import os
from pm4py.objects.log.util import dataframe_utils
from pm4py.algo.conformance.token_based_replay import algorithm as token_based_replay
from pm4py.objects.petri import visualization as vis_pn
from collections import Counter


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])

    # Step 1: Identify top 3 resources by event frequency
    top_resources = log_df['org:resource'].value_counts().nlargest(3).index.tolist()
    filtered_log_df = log_df[log_df['org:resource'].isin(top_resources)]

    # Step 2: Load the reference Petri net
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log_df)

    # Step 3: Token-based replay to identify non-fit cases
    replay_results = token_based_replay.apply(filtered_log_df, petri_net, initial_marking, final_marking)
    non_fit_cases = [case for case, result in replay_results.items() if result['fit'] == False]

    # Step 4: Calculate average throughput time for non-fit cases
    non_fit_case_ids = set(non_fit_cases)
    throughput_times = []
    for case_id in non_fit_case_ids:
        case_events = filtered_log_df[filtered_log_df['case:concept:name'] == case_id]
        if not case_events.empty:
            start_time = case_events['time:timestamp'].min()
            end_time = case_events['time:timestamp'].max()
            throughput_time = (end_time - start_time).total_seconds() / 3600  # in hours
            throughput_times.append(throughput_time)

    average_throughput_time = sum(throughput_times) / len(throughput_times) if throughput_times else 0

    # Step 5: Identify the dominant variant among non-fit cases
    non_fit_variants = filtered_log_df[filtered_log_df['case:concept:name'].isin(non_fit_case_ids)]['concept:name'].value_counts()
    dominant_variant = non_fit_variants.idxmax() if not non_fit_variants.empty else None

    # Step 6: Prepare final answer
    final_answer = {
        'resource': top_resources,
        'conformance': {
            'non_fit_cases': len(non_fit_case_ids),
            'average_throughput_time': average_throughput_time,
            'dominant_variant': dominant_variant
        },
        'performance': {},
        'behavior_variant': {}
    }

    # Save the Petri net visualization
    png_path = 'output/petri_net_visualization.png'
    vis_pn.apply(petri_net, initial_marking, final_marking, png_path)
    print(f'OUTPUT_FILE_LOCATION: {png_path}')  

    # Save the final answer as JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')

    print(json.dumps(final_answer, ensure_ascii=False))