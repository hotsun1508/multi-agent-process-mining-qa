import os
import pandas as pd
import pm4py
import json


def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Flatten the OCEL to get the items view
    flattened_items = pm4py.ocel_flattening(ocel, 'items')
    
    # Step 2: Identify the most dominant variant
    variant_counts = flattened_items['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    
    # Step 3: Filter cases for the dominant variant
    dominant_cases = flattened_items[flattened_items['concept:name'] == dominant_variant]
    
    # Step 4: Discover a Petri net from the dominant variant cases
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(dominant_cases)
    
    # Save the Petri net visualization
    petri_net_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(petri_net, petri_net_path)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_path}')  
    
    # Step 5: Perform token-based replay on the full flattened items view
    replay_results = pm4py.replay_log(dominant_cases, petri_net, initial_marking)
    
    # Step 6: Calculate average case duration of non-fit cases
    non_fit_cases = [case for case in replay_results if not case['fit']]
    non_fit_durations = [case['duration'] for case in non_fit_cases]
    average_non_fit_duration = sum(non_fit_durations) / len(non_fit_durations) if non_fit_durations else 0
    
    # Step 7: Count events linked to non-fit cases in the raw OCEL
    non_fit_case_ids = [case['case_id'] for case in non_fit_cases]
    non_fit_events = ocel.events[ocel.events['case_id'].isin(non_fit_case_ids)]
    
    # Count events linked to both items and customers
    linked_events = non_fit_events[(non_fit_events['type'] == 'items') | (non_fit_events['type'] == 'customers')]
    count_linked_events = linked_events.shape[0]
    
    # Step 8: Prepare final answer
    final_answer = {
        'average_non_fit_duration': average_non_fit_duration,
        'count_linked_events': count_linked_events,
        'dominant_variant': dominant_variant
    }
    
    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))