import pm4py
import pandas as pd
import numpy as np
import json
import os
import statistics
from collections import Counter

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Step 1: Identify the strongest working Together pair of resources
    resource_pairs = log_df.groupby(["case:concept:name", "org:resource"])['case:concept:name'].nunique().reset_index()
    resource_pairs.columns = ["case_id", "resource", "case_count"]
    resource_pairs = resource_pairs.groupby(["resource"])['case_count'].sum().reset_index()
    resource_pairs = resource_pairs.sort_values(by="case_count", ascending=False)
    strongest_pair = resource_pairs.iloc[0]["resource"]
    
    # Step 2: Select the cases involving that pair
    filtered_cases = log_df[log_df["org:resource"] == strongest_pair]
    
    # Step 3: Identify delayed cases of that variant whose total case duration exceeds the overall average case duration
    case_durations = log_df.groupby("case:concept:name")[["time:timestamp"]].agg(["min", "max"])
    case_durations.columns = ["min", "max"]
    case_durations["duration"] = (case_durations["max"] - case_durations["min"]).dt.total_seconds() / 60
    average_duration = case_durations["duration"].mean()
    delayed_cases = case_durations[case_durations["duration"] > average_duration].index.tolist()
    delayed_cases_df = log_df[log_df["case:concept:name"].isin(delayed_cases)]
    
    # Step 4: Determine the dominant variant among those delayed cases
    dominant_variant = delayed_cases_df["concept:name"].value_counts().idxmax()
    dominant_variant_cases = delayed_cases_df[delayed_cases_df["concept:name"] == dominant_variant]
    
    # Step 5: Discover a Petri net from that dominant-variant subset
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(dominant_variant_cases)
    pm4py.save_vis_petri_net(petri_net, "output/petri_net.png")
    print("OUTPUT_FILE_LOCATION: output/petri_net.png")
    
    # Step 6: Calculate the token-based replay fitness of that model on the pair-filtered subset
    fitness = pm4py.fitness_token_based_replay(dominant_variant_cases, petri_net, initial_marking, final_marking)
    
    # Final answer
    final_answer = {
        "strongest_pair": strongest_pair,
        "average_case_duration": average_duration,
        "dominant_variant": dominant_variant,
        "fitness": fitness
    }
    print(json.dumps(final_answer, ensure_ascii=False))