import pm4py
import pandas as pd
import json
import os
import statistics

def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the items view
    flat_items = pm4py.ocel_flattening(ocel, 'items')
    
    # Calculate case durations and filter delayed cases
    case_durations = flat_items.groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).reset_index()
    average_duration = case_durations['time:timestamp'].mean()
    delayed_cases = case_durations[case_durations['time:timestamp'] > average_duration]['case:concept:name']
    
    # Filter the flattened items view for delayed cases
    delayed_items = flat_items[flat_items['case:concept:name'].isin(delayed_cases)]
    
    # Identify the most dominant variant in the delayed cases
    variant_counts = delayed_items['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    
    # Filter delayed items for the dominant variant
    dominant_delayed_items = delayed_items[delayed_items['concept:name'] == dominant_variant]
    
    # Discover a Petri net from the dominant delayed cases
    petri_net = pm4py.discover_petri_net_inductive(dominant_delayed_items)
    png_path = 'output/dominant_variant_petri_net.png'
    pm4py.save_vis_petri_net(petri_net, png_path)
    print(f'OUTPUT_FILE_LOCATION: {png_path}')  
    
    # Compute token-based replay fitness on the full flattened items view
    fitness = pm4py.fitness_token_based(dominant_delayed_items, petri_net)
    
    # Count events linked to delayed dominant-variant subset that are linked to both items and customers
    raw_events = ocel.events
    delayed_event_ids = dominant_delayed_items['ocel:eid'].unique()
    joint_count = raw_events[(raw_events['ocel:eid'].isin(delayed_event_ids)) & (raw_events['ocel:oid'].isin(ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid'])) & (raw_events['ocel:oid'].isin(ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid']))].shape[0]
    
    # Prepare final answer
    final_answer = {
        'average_case_duration': average_duration,
        'dominant_variant': dominant_variant,
        'fitness': fitness,
        'joint_event_count': joint_count
    }
    
    # Save final answer to JSON
    with open('output/final_benchmark_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_benchmark_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))