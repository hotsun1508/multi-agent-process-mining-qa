import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    # Convert event log to dataframe
    log_df = pm4py.convert_to_dataframe(event_log)
    # Get the frequency of each variant
    variants = log_df.groupby(['case:concept:name', 'concept:name']).size().reset_index(name='count')
    # Get the top 20% most frequent variants
    top_20_percent_threshold = variants['count'].quantile(0.8)
    top_variants = variants[variants['count'] >= top_20_percent_threshold]
    top_variant_cases = top_variants['case:concept:name'].unique()
    
    # Filter the original log for top variant cases
    filtered_log = log_df[log_df['case:concept:name'].isin(top_variant_cases)]
    
    # Discover the Petri net model from the event log
    net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log)
    
    # Perform token-based replay
    fitness = pm4py.fitness_token_based_replay(filtered_log, net, initial_marking, final_marking)
    
    # Count cases not fitting under token-based replay
    not_fit_cases = sum(1 for case in fitness if fitness[case]['fit'] == False)
    
    # Prepare final answer
    final_answer = {'not_fit_cases': not_fit_cases}
    
    # Save final answer to JSON file
    with open('output/benchmark_result.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/benchmark_result.json')
    
    # Print final answer
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()