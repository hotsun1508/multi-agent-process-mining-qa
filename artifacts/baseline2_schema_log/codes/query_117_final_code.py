import pm4py
import json
import pandas as pd
import numpy as np
import os


def main():
    ocel = ACTIVE_LOG
    # Step 1: Flatten the OCEL to get the orders view
    flat_orders = pm4py.ocel_flattening(ocel, 'orders')
    
    # Step 2: Discover the Petri net using the Inductive Miner on the top 20% most frequent variants
    variant_counts = flat_orders['case:concept:name'].value_counts()
    top_20_percent_variants = variant_counts.head(int(len(variant_counts) * 0.2)).index.tolist()
    filtered_log = flat_orders[flat_orders['case:concept:name'].isin(top_20_percent_variants)]
    petri_net = pm4py.discover_petri_net_inductive(filtered_log)
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    
    # Step 3: Perform token-based replay to identify non-fit cases
    fitness = pm4py.fitness_token_based_replay(filtered_log, petri_net)
    non_fit_cases = [case for case, fit in fitness.items() if not fit]
    
    # Step 4: Analyze non-fit cases to find the most dominant variant and calculate their average duration
    non_fit_log = filtered_log[filtered_log['case:concept:name'].isin(non_fit_cases)]
    dominant_variant = non_fit_log['case:concept:name'].value_counts().idxmax()
    average_duration = non_fit_log['time:timestamp'].max() - non_fit_log['time:timestamp'].min()
    average_duration = average_duration.total_seconds() / len(non_fit_cases) if len(non_fit_cases) > 0 else 0
    
    # Step 5: Count events linked to non-fit cases that are linked to both orders and items in the raw OCEL
    non_fit_case_ids = non_fit_log['case:concept:name'].unique()
    non_fit_events = ocel.events[ocel.events['case:concept:name'].isin(non_fit_case_ids)]
    linked_events = non_fit_events[non_fit_events['ocel:type'].isin(['orders', 'items'])]
    count_linked_events = linked_events.shape[0]
    
    # Step 6: Save all outputs and the final benchmark answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'average_duration': average_duration,
        'count_linked_events': count_linked_events
    }
    with open('output/final_benchmark_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_benchmark_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))