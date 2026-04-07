import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    # Convert event log to DataFrame
    log_df = pm4py.convert_to_dataframe(event_log)
    
    # Get the top 20% most frequent variants
    variant_counts = log_df.groupby(['case:concept:name']).size().reset_index(name='counts')
    top_20_percent_count = variant_counts['counts'].quantile(0.8)
    top_variants = variant_counts[variant_counts['counts'] >= top_20_percent_count]['case:concept:name']
    
    # Filter the log for top variants
    filtered_log = log_df[log_df['case:concept:name'].isin(top_variants)]
    
    # Load the reference Petri net model (assumed to be pre-loaded)
    # Reference model should be defined or loaded here
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log)
    
    # Calculate token-based replay precision score
    precision_score = pm4py.evaluate_replay_precision(filtered_log, petri_net, initial_marking, final_marking)
    
    # Save the precision score to a JSON-serializable dictionary
    final_answer = {'token_based_replay_precision': precision_score}
    
    # Save the result to a JSON file
    output_path = 'output/replay_precision.json'
    with open(output_path, 'w') as f:
        json.dump(final_answer, f, ensure_ascii=False)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  
    
    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))