import pm4py
import pandas as pd
import json
import os
from pm4py.objects.log.util import dataframe_utils
from pm4py.algo.conformance.token_based_replay import algorithm as token_based_replay
from pm4py.objects.petri import petri_net as pn


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])

    # Step 1: Identify top 3 resources by event frequency
    top_resources = log_df['org:resource'].value_counts().nlargest(3).index.tolist()
    filtered_log_df = log_df[log_df['org:resource'].isin(top_resources)]

    # Step 2: Load the reference Petri net
    petri_net = pm4py.read_pnml('path_to_petri_net.pnml')  # Adjust the path accordingly

    # Step 3: Token-based replay to identify non-fit cases
    non_fit_cases = []
    for case_id, case_df in filtered_log_df.groupby('case:concept:name'):
        case_events = case_df[['concept:name', 'time:timestamp']].values.tolist()
        replay_result = token_based_replay.apply(case_events, petri_net)
        if not replay_result['fit']:
            non_fit_cases.append(case_id)

    # Step 4: Calculate average throughput time for non-fit cases
    non_fit_log_df = filtered_log_df[filtered_log_df['case:concept:name'].isin(non_fit_cases)]
    non_fit_log_df['time:timestamp'] = pd.to_datetime(non_fit_log_df['time:timestamp'])
    throughput_times = non_fit_log_df.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
    throughput_times['throughput_time'] = (throughput_times['time:timestamp']['max'] - throughput_times['time:timestamp']['min']).dt.total_seconds() / 3600.0
    average_throughput_time = throughput_times['throughput_time'].mean()

    # Step 5: Identify the dominant variant among non-fit cases
    dominant_variant = non_fit_log_df['concept:name'].value_counts().idxmax()

    # Step 6: Prepare final answer
    final_answer = {
        'average_throughput_time': average_throughput_time,
        'dominant_variant': dominant_variant,
        'non_fit_cases': non_fit_cases
    }

    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')

    # Save any optional figures, tables, model files, or auxiliary files under output/

    print(json.dumps(final_answer, ensure_ascii=False))