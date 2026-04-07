import pm4py
import pandas as pd
import json
import os
import pickle


def main():
    event_log = ACTIVE_LOG
    # Convert the event log to a case dataframe
    df = pm4py.convert_to_dataframe(event_log)
    
    # Get the top-3 most frequent variants
    variant_counts = df.groupby('case:concept:name')['concept:name'].apply(list).value_counts().head(3)
    top_variants = variant_counts.index.tolist()
    
    # Discover the Petri net model using the Inductive Miner algorithm
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(df[df['case:concept:name'].isin(top_variants)])
    
    # Count the number of places, transitions, and arcs in the Petri net
    num_places = len(petri_net.places)
    num_transitions = len(petri_net.transitions)
    num_arcs = len(petri_net.arcs)
    
    # Save the Petri net model as a .pkl file
    petri_net_file_path = 'output/petri_net_model.pkl'
    with open(petri_net_file_path, 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_file_path}')  
    
    # Prepare the JSON-serializable dictionary with the counts
    result_dict = {
        'primary_answer_in_csv_log': True,
        'result_type': 'single',
        'view': 'event_log',
        'result_schema': {
            'process_discovery': 'petri_net'
        },
        'artifacts_schema': [
            'output/* (optional auxiliary artifacts such as png/csv/pkl/json)'
        ],
        'places': num_places,
        'transitions': num_transitions,
        'arcs': num_arcs
    }
    
    # Write the result to a JSON file
    result_json_path = 'output/result.json'
    with open(result_json_path, 'w') as json_file:
        json.dump(result_dict, json_file)
    print(f'OUTPUT_FILE_LOCATION: {result_json_path}')  
    
    print(json.dumps(result_dict, ensure_ascii=False))