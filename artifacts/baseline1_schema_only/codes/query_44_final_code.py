import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    
    # Step 1: Identify the top 5 resources by event frequency
    top_resources = log_df['org:resource'].value_counts().nlargest(5).index.tolist()
    
    # Step 2: Select cases involving at least one of the top resources
    filtered_cases = log_df[log_df['org:resource'].isin(top_resources)]
    case_ids = filtered_cases['case:concept:name'].unique()
    filtered_log = event_log[case_ids]
    
    # Step 3: Determine the dominant variant in that subset
    variants = pm4py.get_variants(filtered_log)
    dominant_variant = max(variants.items(), key=lambda x: x[1])[0]
    
    # Step 4: Discover a Petri net from the cases of that variant
    variant_log = filtered_log[filtered_log['concept:name'].isin(dominant_variant)]
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(variant_log)
    
    # Save the Petri net visualization
    petri_net_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, petri_net_path)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_path}')  
    
    # Step 5: Check how many cases are fit under token-based replay
    fitness = pm4py.fitness_token_based_replay(variant_log, petri_net, initial_marking)
    fit_cases_count = sum(1 for case in fitness if fitness[case]['fit'])
    
    # Final answer
    final_answer = {
        'resource': top_resources,
        'behavior_variant': dominant_variant,
        'process_discovery': 'Petri net discovered',
        'conformance': fit_cases_count
    }
    
    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))