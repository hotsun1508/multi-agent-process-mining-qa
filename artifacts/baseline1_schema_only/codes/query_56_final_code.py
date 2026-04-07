import pm4py
import pandas as pd
import json
import os
import statistics
from collections import Counter

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Step 1: Identify the strongest working Together pair of resources
    resource_pairs = log_df.groupby(["case:concept:name", "org:resource"]).size().reset_index(name='count')
    resource_pairs = resource_pairs.groupby('org:resource').agg(lambda x: list(x)).reset_index()
    strongest_pair = resource_pairs[resource_pairs['count'].apply(len).idxmax()]['org:resource']
    
    # Step 2: Select the cases involving that pair
    filtered_cases = log_df[log_df['org:resource'].isin(strongest_pair)]
    
    # Step 3: Identify delayed cases of that variant whose total case duration exceeds the overall average case duration
    case_durations = filtered_cases.groupby("case:concept:name").agg(duration=("time:timestamp", lambda x: (x.max() - x.min()).total_seconds()))
    overall_avg_duration = case_durations['duration'].mean()
    delayed_cases = case_durations[case_durations['duration'] > overall_avg_duration].index.tolist()
    delayed_cases_df = filtered_cases[filtered_cases['case:concept:name'].isin(delayed_cases)]
    
    # Step 4: Determine the dominant variant among those delayed cases
    dominant_variant = delayed_cases_df.groupby("case:concept:name").size().idxmax()
    dominant_variant_df = delayed_cases_df[delayed_cases_df['case:concept:name'] == dominant_variant]
    
    # Step 5: Discover a Petri net from that dominant-variant subset
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(dominant_variant_df)
    petri_net_path = "output/petri_net.png"
    pm4py.save_vis_petri_net(petri_net, petri_net_path)
    print(f"OUTPUT_FILE_LOCATION: {petri_net_path}")
    
    # Step 6: Calculate the token-based replay fitness of that model on the pair-filtered subset
    fitness = pm4py.fitness_token_based_replay(dominant_variant_df, petri_net, initial_marking, final_marking)
    
    # Final answer
    final_answer = {
        "resource": strongest_pair,
        "performance": {
            "overall_avg_duration": overall_avg_duration,
            "delayed_cases_count": len(delayed_cases),
            "dominant_variant": dominant_variant,
            "fitness": fitness
        },
        "behavior_variant": dominant_variant,
        "process_discovery": petri_net_path,
        "conformance": fitness
    }
    print(json.dumps(final_answer, ensure_ascii=False))