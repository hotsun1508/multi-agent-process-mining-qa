import pm4py
import pandas as pd
import numpy as np
import json
import os
import statistics

def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the orders view
    flat_orders = pm4py.ocel_flattening(ocel, 'orders')
    
    # Discover the variants and their frequencies
    variants = flat_orders['concept:name'].value_counts()
    top_20_percent_count = int(len(variants) * 0.2)
    top_variants = variants.nlargest(top_20_percent_count).index.tolist()
    
    # Filter the flattened log for the top variants
    filtered_flat_orders = flat_orders[flat_orders['concept:name'].isin(top_variants)]
    
    # Calculate case durations
    case_durations = filtered_flat_orders.groupby('case:concept:name')['time:timestamp'].max() - filtered_flat_orders.groupby('case:concept:name')['time:timestamp'].min()
    average_duration = case_durations.mean()
    
    # Discover the Petri net using the Inductive Miner
    petri_net = pm4py.discover_petri_net_inductive(filtered_flat_orders)
    
    # Perform token-based replay
    replay_result = pm4py.replay_log(petri_net, filtered_flat_orders)
    
    # Isolate non-fit cases
    non_fit_cases = [case for case in replay_result if case['fit'] == False]
    non_fit_count = len(non_fit_cases)
    
    # Calculate percentage of non-fit cases exceeding average duration
    non_fit_case_ids = [case['case'] for case in non_fit_cases]
    non_fit_durations = case_durations[case_durations.index.isin(non_fit_case_ids)]
    non_fit_exceeding_avg = non_fit_durations[non_fit_durations > average_duration]
    percentage_exceeding_avg = (len(non_fit_exceeding_avg) / non_fit_count * 100) if non_fit_count > 0 else 0
    
    # Count events linked to non-fit cases in raw OCEL
    linked_events = ocel.events[ocel.events['case:concept:name'].isin(non_fit_case_ids)]
    linked_events_count = linked_events[(linked_events['ocel:type'] == 'orders') | (linked_events['ocel:type'] == 'customers')].shape[0]
    
    # Save outputs
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    
    # Prepare final answer
    final_answer = {
        'non_fit_count': non_fit_count,
        'percentage_exceeding_avg': percentage_exceeding_avg,
        'linked_events_count': linked_events_count
    }
    with open('output/final_benchmark.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_benchmark.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))