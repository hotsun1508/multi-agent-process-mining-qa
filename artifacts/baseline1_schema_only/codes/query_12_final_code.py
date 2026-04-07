import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    
    # Get the frequency of each variant
    variant_counts = log_df.groupby(['case:concept:name', 'concept:name']).size().reset_index(name='count')
    top_variants = variant_counts.groupby('concept:name')['count'].sum().nlargest(int(len(variant_counts) * 0.2)).index.tolist()
    
    # Filter the log for the top 20% variants
    filtered_log = log_df[log_df['concept:name'].isin(top_variants)]
    
    # Discover the Petri net model
    net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log)
    
    # Perform token-based replay
    fitness = pm4py.fitness_token_based_replay(filtered_log, net, initial_marking, final_marking)
    
    # Count cases not fitting under token-based replay
    not_fit_cases = sum(1 for case in fitness if not fitness[case]['fit'])
    
    # Prepare final answer
    final_answer = {'not_fit_cases': not_fit_cases}
    
    # Save the final answer to a JSON file
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()