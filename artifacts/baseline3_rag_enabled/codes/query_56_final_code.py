import os
import json
import pandas as pd
import pm4py
from pm4py.algo.discovery import petri_net
from pm4py.algo.evaluation import replay_fitness


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    
    # Step 1: Identify the strongest working Together pair of resources
    resource_pairs = log_df.groupby(['org:resource'])['case:concept:name'].nunique().reset_index()
    resource_pairs.columns = ['resource', 'case_count']
    strongest_pair = resource_pairs.nlargest(2, 'case_count')
    strongest_pair = strongest_pair['resource'].values.tolist()
    
    # Step 2: Select the cases involving that pair
    filtered_cases = log_df[log_df['org:resource'].isin(strongest_pair)]
    
    # Step 3: Identify delayed cases of that variant whose total case duration exceeds the overall average case duration
    case_durations = filtered_cases.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
    case_durations['duration'] = case_durations['time:timestamp']['max'] - case_durations['time:timestamp']['min']
    overall_avg_duration = case_durations['duration'].mean()
    delayed_cases = case_durations[case_durations['duration'] > overall_avg_duration].index.tolist()
    
    # Step 4: Determine the dominant variant among those delayed cases
    delayed_cases_df = filtered_cases[filtered_cases['case:concept:name'].isin(delayed_cases)]
    dominant_variant = delayed_cases_df['concept:name'].value_counts().idxmax()
    
    # Step 5: Discover a Petri net from that dominant-variant subset
    dominant_variant_cases = delayed_cases_df[delayed_cases_df['concept:name'] == dominant_variant]
    petri_net_model, initial_marking, final_marking = pm4py.discover_petri_net_inductive(dominant_variant_cases)
    pm4py.save_vis_petri_net(petri_net_model, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    
    # Step 6: Calculate the token-based replay fitness of that model on the pair-filtered subset
    fitness = replay_fitness.apply(filtered_cases, petri_net_model, initial_marking, final_marking)
    
    # Final answer
    final_answer = {
        'strongest_pair': strongest_pair,
        'overall_avg_duration': overall_avg_duration,
        'dominant_variant': dominant_variant,
        'fitness': fitness
    }
    print(json.dumps(final_answer, ensure_ascii=False))