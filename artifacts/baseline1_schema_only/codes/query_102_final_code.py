import pm4py
import json
import os
import statistics
import pickle

def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the packages view
    flat_log = pm4py.ocel_flattening(ocel, 'packages')
    
    # Calculate case durations and isolate delayed cases
    case_durations = flat_log.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
    case_durations['duration'] = case_durations['time:timestamp']['max'] - case_durations['time:timestamp']['min']
    average_duration = case_durations['duration'].mean().total_seconds()
    delayed_cases = case_durations[case_durations['duration'].dt.total_seconds() > average_duration].index.tolist()
    
    # Filter the log for delayed cases
    delayed_log = flat_log[flat_log['case:concept:name'].isin(delayed_cases)]
    
    # Identify the most dominant variant in the delayed cases
    variant_counts = delayed_log['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    
    # Filter the delayed log for the dominant variant
    dominant_delayed_log = delayed_log[delayed_log['concept:name'] == dominant_variant]
    
    # Discover a Petri net from the dominant delayed cases
    petri_net = pm4py.discover_petri_net_inductive(dominant_delayed_log)
    
    # Save the Petri net model
    model_path = 'output/model.pkl'
    with open(model_path, 'wb') as f:
        pickle.dump(petri_net, f)
    print(f'OUTPUT_FILE_LOCATION: {model_path}')  
    
    # Compute token-based replay fitness on the full flattened packages view
    initial_marking, final_marking = pm4py.get_initial_final_marking(petri_net)
    fitness = pm4py.fitness_token_based_replay(flat_log, petri_net, initial_marking, final_marking)
    
    # Save the fitness results
    fitness_path = 'output/fitness.json'
    with open(fitness_path, 'w') as f:
        json.dump(fitness, f)
    print(f'OUTPUT_FILE_LOCATION: {fitness_path}')  
    
    # Prepare final answer
    final_answer = {
        'average_case_duration': average_duration,
        'delayed_cases_count': len(delayed_cases),
        'dominant_variant': dominant_variant,
        'fitness': fitness
    }
    print(json.dumps(final_answer, ensure_ascii=False))