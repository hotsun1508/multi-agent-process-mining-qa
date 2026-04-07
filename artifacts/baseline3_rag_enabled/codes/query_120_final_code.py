import os
import pandas as pd
import pm4py
import json
import statistics

def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Flatten the OCEL for items
    flattened_items = pm4py.ocel_flattening(ocel, 'items')
    
    # Step 2: Identify the most dominant variant in the flattened items view
    variant_counts = flattened_items['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    
    # Step 3: Count events linked to the cases of that variant in the raw OCEL
    raw_events = ocel.events
    variant_cases = flattened_items[flattened_items['concept:name'] == dominant_variant]['case:concept:name'].unique()
    event_count = len(raw_events[(raw_events['ocel:oid'].isin(variant_cases)) & (raw_events['ocel:type'].isin(['items', 'orders']))])
    
    # Step 4: Filter cases based on duration
    case_durations = flattened_items.groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).reset_index()
    average_duration = case_durations['time:timestamp'].mean()
    long_cases = case_durations[case_durations['time:timestamp'] > average_duration]['case:concept:name']
    delayed_variant_cases = [case for case in variant_cases if case in long_cases.values]
    
    # Step 5: Discover a Petri net from the delayed dominant-variant subset
    delayed_flattened = flattened_items[flattened_items['case:concept:name'].isin(delayed_variant_cases)]
    petri_net = pm4py.discover_petri_net_inductive(delayed_flattened)
    
    # Step 6: Compute token-based replay fitness on the full flattened items view
    fitness = pm4py.fitness_token_based_replay(petri_net, flattened_items)
    
    # Save outputs
    pm4py.save_vis_petri_net(petri_net, os.path.join(output_dir, 'petri_net.png'))
    with open(os.path.join(output_dir, 'fitness.json'), 'w') as f:
        json.dump(fitness, f)
    
    # Final answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'event_count': event_count,
        'exceeding_cases_ratio': len(long_cases) / len(variant_cases) if len(variant_cases) > 0 else 0,
        'petri_net': 'petri_net.png'
    }
    print(json.dumps(final_answer, ensure_ascii=False))