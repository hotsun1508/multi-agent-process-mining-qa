import pm4py
import pandas as pd
import json
import os
import pickle

def main():
    event_log = ACTIVE_LOG
    # Step 1: Convert the event log to a case dataframe
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    
    # Step 2: Discover the Petri net model using the Inductive Miner algorithm
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(log_df)
    
    # Step 3: Extract the number of places, transitions, and arcs from the discovered Petri net
    num_places = len(petri_net.places)
    num_transitions = len(petri_net.transitions)
    num_arcs = len(petri_net.arcs)
    
    # Step 4: Save the Petri net model as a .pkl file
    petri_net_filename = 'output/petri_net_model.pkl'
    with open(petri_net_filename, 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_filename}')  
    
    # Step 5: Prepare the final benchmark answer as a JSON-serializable dictionary
    answer = {
        'primary_answer_in_csv_log': True,
        'result_type': 'single',
        'view': 'event_log',
        'result_schema': {'process_discovery': 'petri_net'},
        'artifacts_schema': ['output/* (optional auxiliary artifacts such as png/csv/pkl/json)'],
        'metrics': {
            'num_places': num_places,
            'num_transitions': num_transitions,
            'num_arcs': num_arcs
        }
    }
    
    # Write the final answer to a JSON file
    answer_filename = 'output/benchmark_answer.json'
    with open(answer_filename, 'w', encoding='utf-8') as f:
        json.dump(answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {answer_filename}')  
    
    print(json.dumps(answer, ensure_ascii=False))