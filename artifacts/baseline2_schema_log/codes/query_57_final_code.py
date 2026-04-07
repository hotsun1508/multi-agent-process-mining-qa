import pm4py
import pandas as pd
import json
import os
import statistics

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    
    # Step 1: Identify variants occurring exactly once
    variant_counts = log_df.groupby(['case:concept:name'])['concept:name'].apply(lambda x: tuple(x)).value_counts()
    unique_variants = variant_counts[variant_counts == 1].index.tolist()
    unique_cases = log_df[log_df.groupby(['case:concept:name'])['concept:name'].transform(lambda x: tuple(x)).isin(unique_variants)]
    
    # Step 2: Find top 5 resources in that subset
    top_resources = unique_cases['org:resource'].value_counts().head(5)
    most_frequent_resource = top_resources.idxmax()
    
    # Step 3: Keep only cases involving the most frequent resource
    filtered_cases = unique_cases[unique_cases['org:resource'] == most_frequent_resource]
    
    # Step 4: Identify delayed cases with total case duration exceeding average duration
    case_durations = filtered_cases.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = (case_durations['max'] - case_durations['min']).dt.total_seconds() / 3600
    average_duration = case_durations['duration'].mean()
    delayed_cases = case_durations[case_durations['duration'] > average_duration].index.tolist()
    delayed_cases_df = filtered_cases[filtered_cases['case:concept:name'].isin(delayed_cases)]
    
    # Step 5: Discover a Petri net from those delayed cases
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(delayed_cases_df)
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    
    # Step 6: Calculate token-based replay fitness
    fitness = pm4py.fitness_token_based(petri_net, initial_marking, final_marking, delayed_cases_df)
    
    # Prepare final answer
    final_answer = {
        'unique_variants_count': len(unique_variants),
        'top_resources': top_resources.to_dict(),
        'most_frequent_resource': most_frequent_resource,
        'delayed_cases_count': len(delayed_cases_df),
        'average_case_duration': average_duration,
        'fitness': fitness
    }
    
    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))