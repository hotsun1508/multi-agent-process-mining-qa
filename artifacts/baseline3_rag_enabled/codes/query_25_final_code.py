import pm4py
import json
import os
import pickle

def main():
    event_log = ACTIVE_LOG
    # Step 1: Convert the event log to a case dataframe
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    
    # Step 2: Discover a Petri net using the Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(log_df)
    
    # Step 3: Calculate the token-based replay fitness
    fitness_results = pm4py.fitness_token_based_replay(log=log_df, petri_net=petri_net, initial_marking=initial_marking, final_marking=final_marking)
    
    # Step 4: Prepare the final result dictionary
    result_dict = {
        "primary_answer_in_csv_log": True,
        "result_type": "composite",
        "view": "event_log",
        "result_schema": {
            "process_discovery": "petri_net",
            "conformance": "conformance_summary"
        },
        "metrics": {
            "average_trace_fitness": fitness_results['average_trace_fitness'],
            "log_fitness": fitness_results['log_fitness'],
            "percentage_of_fitting_traces": fitness_results['percentage_of_fitting_traces']
        },
        "artifacts_schema": ["output/* (optional auxiliary artifacts such as png/csv/pkl/json)"]
    }
    
    # Step 5: Save the final result dictionary to a JSON file
    with open('output/result.json', 'w', encoding='utf-8') as f:
        json.dump(result_dict, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/result.json")
    
    # Step 6: Save the Petri net model as a .pkl file
    petri_net_filename = 'output/petri_net_model.pkl'
    with open(petri_net_filename, 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)
    print(f"OUTPUT_FILE_LOCATION: {petri_net_filename}")

    return json.dumps(result_dict, ensure_ascii=False)