import os
import json
import pm4py
import pandas as pd


def main():
    event_log = ACTIVE_LOG
    # Step 1: Convert the event log to a case dataframe
    df = pm4py.convert_to_dataframe(event_log)
    
    # Step 2: Identify the top 5 resources by event frequency
    top_resources = df['org:resource'].value_counts().nlargest(5).index.tolist()
    
    # Step 3: Filter cases involving at least one of the top resources
    filtered_cases = df[df['org:resource'].isin(top_resources)]
    
    # Step 4: Determine the dominant variant in the filtered subset
    variants = filtered_cases.groupby('case:concept:name')['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    dominant_variant = variants.idxmax()
    
    # Step 5: Filter the original dataframe for the dominant variant
    dominant_cases = filtered_cases[filtered_cases['case:concept:name'].isin(variants[variants.index == dominant_variant].index)]
    
    # Step 6: Discover a Petri net from the cases of that variant
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(dominant_cases)
    
    # Step 7: Calculate the token-based replay fitness
    fitness_results = pm4py.fitness_token_based_replay(dominant_cases, petri_net, initial_marking, final_marking)
    
    # Step 8: Prepare the final result dictionary
    result_dict = {
        'resource': top_resources,
        'behavior_variant': dominant_variant,
        'process_discovery': 'petri_net',
        'conformance': fitness_results['fit_cases']
    }
    
    # Step 9: Save the Petri net visualization
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    
    # Step 10: Save the final result dictionary to a JSON file
    with open('output/result.json', 'w', encoding='utf-8') as f:
        json.dump(result_dict, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/result.json')
    
    # Step 11: Return the final answer
    print(json.dumps(result_dict, ensure_ascii=False))