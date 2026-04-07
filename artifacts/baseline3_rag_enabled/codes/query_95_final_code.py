import os
import json
import pm4py
import statistics
import pickle

def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Flatten the OCEL for customers
    flattened_customers = pm4py.ocel_flattening(ocel, 'customers')
    
    # Step 2: Calculate case durations
    case_durations = flattened_customers.groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).reset_index()
    average_duration = case_durations['time:timestamp'].mean()
    
    # Step 3: Isolate delayed cases
    delayed_cases = case_durations[case_durations['time:timestamp'] > average_duration]['case:concept:name']
    delayed_sublog = flattened_customers[flattened_customers['case:concept:name'].isin(delayed_cases)]
    
    # Step 4: Discover Petri net from delayed-case sublog
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(delayed_sublog)
    
    # Save the Petri net model
    with open('output/model_delayed.pkl', 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)
    print('OUTPUT_FILE_LOCATION: output/model_delayed.pkl')
    
    # Step 5: Compute token-based replay fitness on the full flattened customers view
    fitness = pm4py.fitness_token_based_replay(petri_net, initial_marking, flattened_customers)
    
    # Save fitness results
    with open('output/fitness_full.json', 'w') as f:
        json.dump(fitness, f)
    print('OUTPUT_FILE_LOCATION: output/fitness_full.json')
    
    # Step 6: Prepare final benchmark answer
    final_answer = {
        'average_case_duration': average_duration,
        'delayed_cases_count': len(delayed_cases),
        'total_cases': len(case_durations),
        'fitness': fitness
    }
    print(json.dumps(final_answer, ensure_ascii=False))