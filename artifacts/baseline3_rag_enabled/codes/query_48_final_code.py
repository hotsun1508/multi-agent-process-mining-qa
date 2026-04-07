import os
import json
import pandas as pd
import pm4py
from pm4py.algo.conformance.tokenreplay import algorithm as token_replay


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    
    # Step 1: Identify the top 3 resources by event frequency
    top_resources = log_df['org:resource'].value_counts().nlargest(3).index.tolist()
    
    # Step 2: Filter the log for cases involving the top 3 resources
    filtered_log_df = log_df[log_df['org:resource'].isin(top_resources)]
    
    # Step 3: Load the reference Petri net
    petri_net, initial_marking, final_marking = pm4py.read_pnml('path_to_reference_petri_net.pnml')
    
    # Step 4: Perform token-based replay
    non_fit_cases = []
    for case_id, case_df in filtered_log_df.groupby('case:concept:name'):
        replay_result = token_replay.apply(case_df, petri_net, initial_marking, final_marking)
        if not replay_result['fit']:
            non_fit_cases.append(case_df)
    
    # Step 5: Calculate average throughput time for non-fit cases
    if non_fit_cases:
        non_fit_cases_df = pd.concat(non_fit_cases)
        non_fit_cases_df['time:timestamp'] = pd.to_datetime(non_fit_cases_df['time:timestamp'])
        throughput_times = non_fit_cases_df.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
        throughput_times['throughput_time'] = (throughput_times['time:timestamp']['max'] - throughput_times['time:timestamp']['min']).dt.total_seconds() / 3600.0
        average_throughput_time = throughput_times['throughput_time'].mean()
    else:
        average_throughput_time = 0
    
    # Step 6: Identify the dominant variant among non-fit cases
    if non_fit_cases:
        dominant_variant = non_fit_cases_df['concept:name'].value_counts().idxmax()
    else:
        dominant_variant = None
    
    # Step 7: Prepare the final answer
    final_answer = {
        'average_throughput_time': average_throughput_time,
        'dominant_variant': dominant_variant
    }
    
    # Step 8: Save the final answer
    with open('output/final_benchmark_answer.json', 'w') as f:
        json.dump(final_answer, f, ensure_ascii=False)
    print('OUTPUT_FILE_LOCATION: output/final_benchmark_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))