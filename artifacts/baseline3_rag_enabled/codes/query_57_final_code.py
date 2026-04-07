import os
import json
import pm4py
import pandas as pd
from pm4py.algo.discovery import petri_net as pn_discovery
from pm4py.algo.evaluation import replay_fitness


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    
    # Step 1: Identify variants occurring exactly once
    variants = pm4py.get_variants(event_log)
    unique_variants = [k for k, v in variants.items() if (v if isinstance(v, int) else len(v)) == 1]
    unique_cases = log_df[log_df['case:concept:name'].isin(unique_variants)]
    
    # Step 2: Find top 5 resources in that subset
    top_resources = unique_cases['org:resource'].value_counts().head(5).index.tolist()
    
    # Step 3: Keep only cases involving the most frequent resource
    most_frequent_resource = unique_cases['org:resource'].value_counts().idxmax()
    filtered_cases = unique_cases[unique_cases['org:resource'] == most_frequent_resource]
    
    # Step 4: Identify delayed cases with total case duration exceeding average duration
    average_duration = log_df.groupby('case:concept:name').apply(lambda x: (x['time:timestamp'].max() - x['time:timestamp'].min()).total_seconds()).mean()
    delayed_cases = filtered_cases.groupby('case:concept:name').filter(lambda x: (x['time:timestamp'].max() - x['time:timestamp'].min()).total_seconds() > average_duration)
    
    # Step 5: Discover a Petri net from those delayed cases
    delayed_event_log = pm4py.convert_to_event_log(delayed_cases)
    petri_net, initial_marking, final_marking = pn_discovery.apply(delayed_event_log)
    pn_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, pn_path)
    print(f'OUTPUT_FILE_LOCATION: {pn_path}')  
    
    # Step 6: Calculate token-based replay fitness
    fitness = replay_fitness.apply(delayed_event_log, petri_net, initial_marking, final_marking)
    
    # Final answer
    final_answer = {
        'variant_count': len(unique_variants),
        'top_resources': top_resources,
        'most_frequent_resource': most_frequent_resource,
        'delayed_case_count': len(delayed_cases),
        'average_case_duration': average_duration,
        'fitness': fitness
    }
    
    with open('output/benchmark_result.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/benchmark_result.json')
    print(json.dumps(final_answer, ensure_ascii=False))