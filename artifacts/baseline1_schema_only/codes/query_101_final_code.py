import pm4py
import pandas as pd
import json
import os
import statistics

def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the orders view
    flat_log = pm4py.ocel_flattening(ocel, 'orders')
    # Get the frequency of each variant
    variant_counts = flat_log['concept:name'].value_counts()
    # Select the top 20% variants by frequency
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()
    # Filter the log for the top variants
    model_building_sublog = flat_log[flat_log['concept:name'].isin(top_variants)]
    # Discover a Petri net from the model-building sublog
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(model_building_sublog)
    # Run token-based replay on the model-building sublog
    fit_cases = pm4py.conformance_token_based_replay(model_building_sublog, petri_net, initial_marking)
    # Isolate non-fit cases
    non_fit_cases = [case for case in fit_cases if not case['fit']]
    # Calculate the average case duration of the model-building sublog
    case_durations = model_building_sublog.groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).tolist()
    average_duration = statistics.mean(case_durations) if case_durations else 0
    # Calculate the percentage of non-fit cases whose duration exceeds the average duration
    non_fit_durations = [case['duration'] for case in non_fit_cases]
    non_fit_exceeding_average = sum(1 for duration in non_fit_durations if duration > average_duration)
    percentage_exceeding_average = (non_fit_exceeding_average / len(non_fit_cases) * 100) if non_fit_cases else 0
    # Save the Petri net visualization
    petri_net_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(petri_net, petri_net_path)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_path}')  
    # Save the final answer
    final_answer = {
        'behavior_variant': top_variants,
        'process_discovery': {'petri_net': petri_net_path},
        'conformance': {'non_fit_cases': len(non_fit_cases), 'percentage_exceeding_average': percentage_exceeding_average},
        'performance': {'average_case_duration': average_duration}
    }
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    print(json.dumps(final_answer, ensure_ascii=False))