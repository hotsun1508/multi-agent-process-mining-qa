import pm4py
import pandas as pd
import json
import os
import pickle
from collections import Counter


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    
    # Calculate throughput time for each case
    log_df['timestamp'] = pd.to_datetime(log_df['time:timestamp'])
    throughput_times = log_df.groupby('case:concept:name')['timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).reset_index()
    throughput_times.columns = ['case:concept:name', 'throughput_time']
    
    # Identify the slowest 10% of cases
    slow_cases_threshold = throughput_times['throughput_time'].quantile(0.9)
    slow_cases = throughput_times[throughput_times['throughput_time'] >= slow_cases_threshold]
    slow_case_ids = slow_cases['case:concept:name'].tolist()
    
    # Filter the original log for slow cases
    slow_log_df = log_df[log_df['case:concept:name'].isin(slow_case_ids)]
    
    # Determine the dominant variant among slow cases
    variant_counts = slow_log_df.groupby('case:concept:name')['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    dominant_variant = variant_counts.idxmax()
    
    # Discover a reference Petri net from the cases of that variant
    filtered_cases = slow_log_df[slow_log_df['case:concept:name'].isin(slow_cases['case:concept:name'])]
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_cases)
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    with open('output/petri_net.pkl', 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)
    print('OUTPUT_FILE_LOCATION: output/petri_net.pkl')
    
    # Identify cases that are not fit under token-based replay
    non_fit_cases = []
    for case_id in slow_case_ids:
        case_log = filtered_cases[filtered_cases['case:concept:name'] == case_id]
        if not pm4py.check_fit_token_based(case_log, petri_net, initial_marking):
            non_fit_cases.append(case_id)
    
    # Get resources from non-fit cases
    non_fit_log_df = log_df[log_df['case:concept:name'].isin(non_fit_cases)]
    top_resources = Counter(non_fit_log_df['org:resource']).most_common(3)
    top_resources_list = [resource[0] for resource in top_resources]
    
    # Prepare final answer
    final_answer = {
        'performance': {
            'slow_cases_count': len(slow_case_ids),
            'dominant_variant': dominant_variant,
        },
        'behavior_variant': dominant_variant,
        'process_discovery': 'output/petri_net.pkl',
        'conformance': {
            'non_fit_cases_count': len(non_fit_cases),
            'top_resources': top_resources_list,
        },
        'resource': top_resources_list,
    }
    print(json.dumps(final_answer, ensure_ascii=False))