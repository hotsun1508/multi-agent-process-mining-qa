import pm4py
import pandas as pd
import numpy as np
import json
import os
import statistics

def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the customers view
    flattened_customers = pm4py.ocel_flattening(ocel, 'customers')
    
    # Calculate case durations
    case_durations = flattened_customers.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = case_durations['max'] - case_durations['min']
    average_duration = case_durations['duration'].mean()
    
    # Get the frequency of each variant
    variant_counts = flattened_customers['concept:name'].value_counts()
    top_20_percent_threshold = variant_counts.quantile(0.8)
    top_variants = variant_counts[variant_counts >= top_20_percent_threshold].index.tolist()
    
    # Filter cases by top variants and duration
    filtered_cases = flattened_customers[flattened_customers['case:concept:name'].isin(top_variants)]
    filtered_cases = filtered_cases[case_durations.loc[filtered_cases['case:concept:name']]['duration'] > average_duration]
    
    # Discover Petri net from the filtered cases
    petri_net = pm4py.discover_petri_net_inductive(filtered_cases)
    png_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(petri_net, png_path)
    print(f'OUTPUT_FILE_LOCATION: {png_path}')  
    
    # Compute token-based replay fitness on the full flattened customers view
    fitness = pm4py.fitness_token_based_replay(petri_net, flattened_customers)
    
    # Count events linked to both customers and employees in the raw OCEL
    joint_event_count = 0
    for event in ocel.events:
        if event['ocel:oid'] in filtered_cases['case:concept:name'].values:
            if event['ocel:type'] == 'customers' and event['ocel:qualifier'] in ocel.relations:
                if any(rel['ocel:oid'] == event['ocel:oid'] and rel['ocel:type'] == 'employees' for rel in ocel.relations):
                    joint_event_count += 1
    
    # Prepare final answer
    final_answer = {
        'behavior_variant': top_variants,
        'performance': average_duration,
        'process_discovery': 'Petri net discovered',
        'conformance': fitness,
        'object_interaction': joint_event_count
    }
    
    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))