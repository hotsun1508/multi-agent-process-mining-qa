import pm4py
import pandas as pd
import json
import os
import statistics

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    
    # Get the frequency of each variant
    variants = log_df.groupby(['case:concept:name', 'concept:name']).size().reset_index(name='counts')
    variant_counts = variants.groupby('concept:name').sum().reset_index()
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count, 'counts')
    
    # Filter the log for the top variants
    top_variant_names = top_variants['concept:name'].tolist()
    filtered_log_df = log_df[log_df['concept:name'].isin(top_variant_names)]
    
    # Discover the Petri net using the Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log_df)
    
    # Save the Petri net visualization
    petri_net_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, petri_net_path)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_path}')
    
    # Token-based replay to find fit cases
    fit_cases = pm4py.replay_fitness(filtered_log_df, petri_net, initial_marking, final_marking)
    
    # Calculate throughput time for fit cases
    fit_case_ids = [case for case, fit in zip(filtered_log_df['case:concept:name'].unique(), fit_cases) if fit]
    throughput_times = []
    for case_id in fit_case_ids:
        case_events = filtered_log_df[filtered_log_df['case:concept:name'] == case_id]
        start_time = case_events['time:timestamp'].min()
        end_time = case_events['time:timestamp'].max()
        throughput_time = (end_time - start_time).total_seconds()
        throughput_times.append(throughput_time)
    
    # Determine the dominant variant among fit cases
    dominant_variant = top_variants.iloc[0]['concept:name'] if top_variants.shape[0] > 0 else None
    median_throughput_time = statistics.median(throughput_times) if throughput_times else 0
    
    # Prepare final answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'median_throughput_time': median_throughput_time
    }
    
    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: output/final_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))