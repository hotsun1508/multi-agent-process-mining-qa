import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    # Convert the event log to a DataFrame
    df = pm4py.convert_to_dataframe(event_log)
    
    # Get the top 20% most frequent variants
    variant_counts = df['case:concept:name'].value_counts()
    top_variants = variant_counts.head(int(len(variant_counts) * 0.2)).index.tolist()
    filtered_df = df[df['case:concept:name'].isin(top_variants)]
    
    # Load the reference Petri net model components
    petri_net, initial_marking, final_marking = pm4py.read_petri_net('Single Agent Baseline 3')
    
    # Calculate the token-based replay fitness
    fitness_results = pm4py.conformance_diagnostics_token_based_replay(
        filtered_df,
        petri_net,
        initial_marking,
        final_marking,
        activity_key='concept:name',
        timestamp_key='time:timestamp',
        case_id_key='case:concept:name'
    )
    
    # Count the number of cases not fitting
    not_fit_cases = sum(1 for result in fitness_results if not result['trace_is_fit'])
    
    # Prepare the result dictionary
    final_answer = {
        'not_fit_cases_count': not_fit_cases,
        'total_cases_evaluated': len(filtered_df['case:concept:name'].unique())
    }
    
    # Save the final result to a CSV file
    output_path = 'output/fitness_results.csv'
    pd.DataFrame([final_answer]).to_csv(output_path, index=False)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  
    
    # Return the final answer as a JSON-serializable dictionary
    print(json.dumps(final_answer, ensure_ascii=False))