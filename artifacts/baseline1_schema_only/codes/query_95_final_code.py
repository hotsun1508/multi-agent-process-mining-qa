import pm4py
import pandas as pd
import json
import os
import pickle
import statistics

def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the customers view
    flattened_customers = pm4py.ocel_flattening(ocel, 'customers')
    # Calculate case durations
    flattened_customers['case_duration'] = flattened_customers.groupby('case:concept:name')['time:timestamp'].transform(lambda x: x.max() - x.min()).dt.total_seconds()
    # Calculate average case duration
    average_case_duration = flattened_customers['case_duration'].mean()
    # Isolate delayed cases
    delayed_cases = flattened_customers[flattened_customers['case_duration'] > average_case_duration]
    # Discover Petri net from delayed-case sublog
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(delayed_cases)
    # Save the Petri net model
    with open('output/model_delayed.pkl', 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)
    print('OUTPUT_FILE_LOCATION: output/model_delayed.pkl')
    # Compute token-based replay fitness on the full flattened customers view
    fitness = pm4py.fitness_token_based_replay(petri_net, initial_marking, flattened_customers)
    # Save fitness results
    with open('output/fitness_full.json', 'w') as f:
        json.dump(fitness, f)
    print('OUTPUT_FILE_LOCATION: output/fitness_full.json')
    # Prepare final answer
    final_answer = {
        'average_case_duration': average_case_duration,
        'delayed_cases_count': len(delayed_cases),
        'fitness': fitness
    }
    print(json.dumps(final_answer, ensure_ascii=False))