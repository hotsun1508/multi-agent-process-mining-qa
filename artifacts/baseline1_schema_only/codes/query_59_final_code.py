import pm4py
import pandas as pd
import json
import os
from pm4py.objects.log.util import dataframe_utils
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.algo.conformance.tokenreplay import algorithm as token_replay


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    # Step 1: Discover Directly-Follows Graph
    dfg, start_activities, end_activities = dfg_discovery.apply(event_log)
    # Step 2: Identify the most frequent edge
    most_frequent_edge = max(dfg.items(), key=lambda x: x[1])
    edge_source, edge_target = most_frequent_edge[0]
    # Step 3: Filter cases containing the most frequent edge
    filtered_cases = log_df[(log_df['concept:name'] == edge_source) | (log_df['concept:name'] == edge_target)]
    filtered_case_ids = filtered_cases['case:concept:name'].unique()
    filtered_log = event_log[filtered_log['case:concept:name'].isin(filtered_case_ids)]
    # Step 4: Determine the dominant variant
    dominant_variant = filtered_log['concept:name'].value_counts().idxmax()
    # Step 5: Token-based replay to identify non-fit cases
    petri_net, initial_marking, final_marking = pm4py.read_pnml('path_to_reference_petri_net.pnml')
    non_fit_cases = token_replay.apply(filtered_log, petri_net, initial_marking, final_marking, variant=token_replay.Variants.TOKEN_BASED)
    # Step 6: Calculate average throughput time of non-fit cases
    non_fit_case_ids = [case['case:concept:name'] for case in non_fit_cases]
    non_fit_df = log_df[log_df['case:concept:name'].isin(non_fit_case_ids)]
    non_fit_df['time:timestamp'] = pd.to_datetime(non_fit_df['time:timestamp'])
    throughput_times = non_fit_df.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
    throughput_times['throughput_time'] = (throughput_times['time:timestamp']['max'] - throughput_times['time:timestamp']['min']).dt.total_seconds() / 3600
    average_throughput_time = throughput_times['throughput_time'].mean()
    # Step 7: Identify top 3 resources in non-fit cases
    top_resources = non_fit_df['org:resource'].value_counts().head(3).to_dict()
    # Final answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'average_throughput_time': average_throughput_time,
        'top_resources': top_resources
    }
    # Save results
    with open('output/results.json', 'w') as f:
        json.dump(final_answer, f, ensure_ascii=False)
    print('OUTPUT_FILE_LOCATION: output/results.json')
    print(json.dumps(final_answer, ensure_ascii=False))