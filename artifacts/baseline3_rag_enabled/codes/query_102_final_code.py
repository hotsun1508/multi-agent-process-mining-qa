import os
import pandas as pd
import json
import pm4py


def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Flatten the OCEL for packages
    flat_packages = pm4py.ocel_flattening(ocel, 'packages')
    
    # Step 2: Calculate case durations and average case duration
    case_durations = flat_packages.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
    case_durations['duration'] = case_durations['time:timestamp']['max'] - case_durations['time:timestamp']['min']
    average_duration = case_durations['duration'].mean()
    
    # Step 3: Isolate delayed cases
    delayed_cases = case_durations[case_durations['duration'] > average_duration].index.tolist()
    delayed_flat_packages = flat_packages[flat_packages['case:concept:name'].isin(delayed_cases)]
    
    # Step 4: Identify the most dominant variant in the delayed cases
    variant_counts = delayed_flat_packages['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    
    # Step 5: Filter delayed cases for the dominant variant
    dominant_delayed_cases = delayed_flat_packages[delayed_flat_packages['concept:name'] == dominant_variant]
    
    # Step 6: Discover a Petri net from the filtered cases
    petri_net = pm4py.discover_petri_net_inductive(dominant_delayed_cases)
    
    # Step 7: Compute token-based replay fitness on the full flattened packages view
    initial_marking = pm4py.get_initial_marking(petri_net)
    final_marking = pm4py.get_final_marking(petri_net)
    fitness = pm4py.fitness_token_based_replay(petri_net, flat_packages, initial_marking, final_marking)
    
    # Step 8: Save the Petri net and fitness results
    with open('output/model.pkl', 'wb') as f:
        pm4py.save_vis_petri_net(petri_net, f)
    print('OUTPUT_FILE_LOCATION: output/model.pkl')
    
    with open('output/fitness.json', 'w') as f:
        json.dump(fitness, f)
    print('OUTPUT_FILE_LOCATION: output/fitness.json')
    
    # Step 9: Prepare final benchmark answer
    final_answer = {
        'exceeding_cases_ratio': len(delayed_cases) / len(case_durations),
        'petri_net': str(petri_net),
        'top_variants': variant_counts.index.tolist()
    }
    print(json.dumps(final_answer, ensure_ascii=False))