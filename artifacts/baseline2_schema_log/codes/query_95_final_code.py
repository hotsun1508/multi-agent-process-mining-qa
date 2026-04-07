import pm4py
import json
import os
import pickle
import statistics

def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the customers view
    flattened_customers = pm4py.ocel_flattening(ocel, 'customers')
    
    # Calculate case durations
    case_durations = flattened_customers.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = case_durations['max'] - case_durations['min']
    average_duration = statistics.mean(case_durations['duration'].dt.total_seconds())
    
    # Isolate cases with duration exceeding the average
    delayed_cases = case_durations[case_durations['duration'].dt.total_seconds() > average_duration].index.tolist()
    delayed_sublog = flattened_customers[flattened_customers['case:concept:name'].isin(delayed_cases)]
    
    # Discover Petri net from the delayed-case sublog
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(delayed_sublog)
    
    # Save the Petri net model
    with open('output/model_delayed.pkl', 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)
    print('OUTPUT_FILE_LOCATION: output/model_delayed.pkl')
    
    # Compute token-based replay fitness on the full flattened customers view
    fitness = pm4py.fitness_token_based_replay(petri_net, initial_marking, flattened_customers)
    
    # Save the fitness result
    with open('output/fitness_full.json', 'w') as f:
        json.dump(fitness, f)
    print('OUTPUT_FILE_LOCATION: output/fitness_full.json')
    
    # Prepare final answer
    final_answer = {
        'average_case_duration': average_duration,
        'delayed_cases_count': len(delayed_cases),
        'fitness': fitness
    }
    print(json.dumps(final_answer, ensure_ascii=False))