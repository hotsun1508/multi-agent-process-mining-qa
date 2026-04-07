import os
import pandas as pd
import json
import pm4py


def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Flatten the OCEL for items
    flattened_items = pm4py.ocel_flattening(ocel, 'items')
    
    # Step 2: Calculate average case duration
    case_durations = flattened_items.groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).reset_index()
    average_duration = case_durations['time:timestamp'].mean()
    
    # Step 3: Filter cases with duration exceeding average
    delayed_cases = case_durations[case_durations['time:timestamp'] > average_duration]['case:concept:name']
    delayed_flattened = flattened_items[flattened_items['case:concept:name'].isin(delayed_cases)]
    
    # Step 4: Identify the most dominant variant in the delayed cases
    variant_counts = delayed_flattened['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    
    # Step 5: Discover a Petri net from the delayed cases belonging to the dominant variant
    dominant_delayed_cases = delayed_flattened[delayed_flattened['concept:name'] == dominant_variant]
    petri_net = pm4py.discover_petri_net_inductive(dominant_delayed_cases)
    
    # Step 6: Compute token-based replay fitness on the full flattened items view
    initial_marking, final_marking = pm4py.get_initial_final_marking(petri_net)
    fitness = pm4py.fitness_token_based_replay(petri_net, flattened_items, initial_marking, final_marking)
    
    # Step 7: Count events linked to the delayed dominant-variant subset in the raw OCEL
    delayed_events = ocel.events[ocel.events['case:concept:name'].isin(delayed_cases) & ocel.events['concept:name'] == dominant_variant]
    joint_count = delayed_events[delayed_events['ocel:oid'].isin(ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid']) & delayed_events['ocel:oid'].isin(ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid'])].shape[0]
    
    # Step 8: Save outputs
    pm4py.save_vis_petri_net(petri_net, os.path.join(output_dir, 'petri_net.png'))
    with open(os.path.join(output_dir, 'fitness.json'), 'w') as f:
        json.dump(fitness, f)
    
    # Final answer
    final_answer = {
        'average_case_duration': average_duration,
        'dominant_variant': dominant_variant,
        'joint_event_count': joint_count,
        'fitness': fitness
    }
    print(json.dumps(final_answer, ensure_ascii=False))