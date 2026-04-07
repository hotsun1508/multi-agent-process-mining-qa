import pm4py
import pandas as pd
import json
import os
import pickle
import statistics

def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the packages view
    flat_log = pm4py.ocel_flattening(ocel, object_type='packages')
    
    # Calculate case durations
    case_durations = flat_log.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = case_durations['max'] - case_durations['min']
    average_duration = case_durations['duration'].mean()
    
    # Isolate delayed cases
    delayed_cases = case_durations[case_durations['duration'] > average_duration].index.tolist()
    delayed_log = flat_log[flat_log['case:concept:name'].isin(delayed_cases)]
    
    # Identify the most dominant variant in the delayed cases
    variant_counts = delayed_log['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    
    # Filter delayed log for the dominant variant
    dominant_delayed_log = delayed_log[delayed_log['concept:name'] == dominant_variant]
    
    # Discover Petri net from the dominant delayed cases
    petri_net = pm4py.discover_petri_net_inductive(dominant_delayed_log)
    
    # Save the Petri net model
    with open('output/model.pkl', 'wb') as f:
        pickle.dump(petri_net, f)
    print('OUTPUT_FILE_LOCATION: output/model.pkl')
    
    # Compute token-based replay fitness on the full flattened packages view
    initial_marking = petri_net['initial_marking']
    final_marking = petri_net['final_marking']
    fitness = pm4py.fitness_token_based_replay(petri_net['petri_net'], flat_log, initial_marking, final_marking)
    
    # Save fitness results
    with open('output/fitness.json', 'w') as f:
        json.dump(fitness, f)
    print('OUTPUT_FILE_LOCATION: output/fitness.json')
    
    # Prepare final answer
    final_answer = {
        'average_case_duration': average_duration,
        'dominant_variant': dominant_variant,
        'delayed_cases_count': len(delayed_cases),
        'fitness': fitness
    }
    print(json.dumps(final_answer, ensure_ascii=False))