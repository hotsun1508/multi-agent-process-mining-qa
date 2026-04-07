import pm4py
import pandas as pd
import json
import os
import statistics

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    
    # Get the frequency of each variant
    variant_counts = log_df.groupby(['case:concept:name']).size().reset_index(name='count')
    top_variants = variant_counts.nlargest(int(len(variant_counts) * 0.2), 'count')['case:concept:name'].tolist()
    
    # Filter the log for the top variants
    filtered_log = log_df[log_df['case:concept:name'].isin(top_variants)]
    
    # Discover the Petri net using the Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log)
    
    # Save the Petri net visualization
    petri_net_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, petri_net_path)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_path}')  
    
    # Token-based replay to find fit cases
    fit_cases = pm4py.conformance_token_based_replay(filtered_log, petri_net, initial_marking, final_marking)
    fit_case_ids = [case['case_id'] for case in fit_cases if case['fit']]
    
    # Calculate throughput time for fit cases
    throughput_times = []
    for case_id in fit_case_ids:
        timestamps = filtered_log[filtered_log['case:concept:name'] == case_id]['time:timestamp']
        if len(timestamps) > 1:
            throughput_time = (timestamps.max() - timestamps.min()).total_seconds()
            throughput_times.append(throughput_time)
    
    # Determine the dominant variant
    dominant_variant = statistics.mode(filtered_log['case:concept:name']) if fit_case_ids else None
    median_throughput_time = statistics.median(throughput_times) if throughput_times else 0
    
    # Prepare final answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'median_throughput_time': median_throughput_time
    }
    
    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))