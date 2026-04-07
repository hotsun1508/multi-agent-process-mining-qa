import pm4py
import pandas as pd
import json
import os
import statistics

def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the items view
    flattened_items = pm4py.ocel_flattening(ocel, 'items')
    
    # Calculate case durations
    case_durations = flattened_items.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = case_durations['max'] - case_durations['min']
    average_duration = case_durations['duration'].mean()
    
    # Filter cases with duration exceeding the average
    delayed_cases = case_durations[case_durations['duration'] > average_duration].index.tolist()
    delayed_flattened = flattened_items[flattened_items['case:concept:name'].isin(delayed_cases)]
    
    # Identify the most dominant variant in the delayed cases
    variant_counts = delayed_flattened['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    
    # Filter delayed cases for the dominant variant
    dominant_delayed_cases = delayed_flattened[delayed_flattened['concept:name'] == dominant_variant]
    
    # Discover a Petri net from the dominant delayed cases
    petri_net = pm4py.discover_petri_net_inductive(dominant_delayed_cases)
    
    # Compute token-based replay fitness on the full flattened items view
    fitness = pm4py.fitness_token_based_replay(petri_net, flattened_items)
    
    # Count events linked to the delayed dominant-variant subset that are linked to both items and customers
    raw_events = ocel.events
    delayed_event_ids = dominant_delayed_cases['ocel:eid'].unique()
    joint_count = raw_events[(raw_events['ocel:eid'].isin(delayed_event_ids)) & 
                              (raw_events['ocel:oid'].isin(ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid'])) & 
                              (raw_events['ocel:oid'].isin(ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid']))].shape[0]
    
    # Save outputs
    output_dir = 'output/'
    os.makedirs(output_dir, exist_ok=True)
    
    # Save Petri net visualization
    pm4py.save_vis_petri_net(petri_net, os.path.join(output_dir, 'petri_net.png'))
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'petri_net.png')}')
    
    # Save fitness result
    fitness_result_path = os.path.join(output_dir, 'fitness_result.json')
    with open(fitness_result_path, 'w') as f:
        json.dump({'fitness': fitness}, f)
    print(f'OUTPUT_FILE_LOCATION: {fitness_result_path}')
    
    # Save joint count result
    joint_count_result_path = os.path.join(output_dir, 'joint_count.json')
    with open(joint_count_result_path, 'w') as f:
        json.dump({'joint_count': joint_count}, f)
    print(f'OUTPUT_FILE_LOCATION: {joint_count_result_path}')
    
    # Final answer
    final_answer = {
        'average_case_duration': average_duration,
        'dominant_variant': dominant_variant,
        'fitness': fitness,
        'joint_count': joint_count
    }
    print(json.dumps(final_answer, ensure_ascii=False))