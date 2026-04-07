import pm4py
import pandas as pd
import os
import json

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log)
    # Load the reference Petri net model components
    petri_net, initial_marking, final_marking = pm4py.load_reference_petri_components('BPI Challenge 2017')
    # Calculate the token-based replay fitness
    fitness_result = pm4py.fitness_token_based_replay(log_df, petri_net, initial_marking, final_marking)
    # Prepare the result dictionary in the required JSON format
    result_dict = {
        'primary_answer_in_csv_log': True,
        'result_type': 'single',
        'view': 'event_log',
        'result_schema': {
            'conformance': {
                'average_trace_fitness': fitness_result['average_trace_fitness'],
                'log_fitness': fitness_result['log_fitness'],
                'percentage_of_fitting_traces': fitness_result['percentage_of_fitting_traces']
            }
        },
        'artifacts_schema': ['output/* (optional auxiliary artifacts such as png/csv/pkl/json)']
    }
    # Save the fitness results as a CSV file
    csv_filename = 'output/fitness_results.csv'
    pd.DataFrame([result_dict['result_schema']['conformance']]).to_csv(csv_filename, index=False)
    print(f'OUTPUT_FILE_LOCATION: {csv_filename}')  
    print(json.dumps(result_dict, ensure_ascii=False))