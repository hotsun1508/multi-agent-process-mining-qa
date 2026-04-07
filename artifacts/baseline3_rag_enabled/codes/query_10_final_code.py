import os
import json
import pandas as pd
import pm4py

def main():
    event_log = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Convert the event log to a DataFrame
    df = pm4py.convert_to_dataframe(event_log)

    # Step 2: Get the top 20% most frequent variants
    variant_counts = df.groupby('case:concept:name').size().reset_index(name='counts')
    top_variants = variant_counts.nlargest(int(len(variant_counts) * 0.2), 'counts')['case:concept:name']
    df_top_variants = df[df['case:concept:name'].isin(top_variants)]

    # Step 3: Load the reference Petri net model components
    petri_net, initial_marking, final_marking = pm4py.read_pnml('Single Agent Baseline 3')

    # Step 4: Calculate the token-based replay fitness
    fitness_result = pm4py.fitness_token_based_replay(df_top_variants, petri_net, initial_marking, final_marking)

    # Step 5: Prepare the result dictionary in the required JSON format
    result_dict = {
        'primary_answer_in_csv_log': True,
        'result_type': 'single',
        'view': 'event_log',
        'result_schema': {
            'conformance': 'conformance_summary'
        },
        'artifacts_schema': ['output/* (optional auxiliary artifacts such as png/csv/pkl/json)'],
        'result': {
            'metrics': {
                'average_trace_fitness': fitness_result['average_trace_fitness'],
                'log_fitness': fitness_result['log_fitness'],
                'percentage_of_fitting_traces': fitness_result['percentage_of_fitting_traces']
            }
        }
    }

    # Step 6: Save the fitness results as a CSV file
    csv_filename = os.path.join(output_dir, 'fitness_results.csv')
    pd.DataFrame([result_dict['result']['metrics']]).to_csv(csv_filename, index=False)
    print(f'OUTPUT_FILE_LOCATION: {csv_filename}')  

    # Step 7: Return the final result as a JSON-serializable dictionary
    print(json.dumps(result_dict, ensure_ascii=False))