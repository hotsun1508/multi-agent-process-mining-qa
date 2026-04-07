import pm4py
import pandas as pd
import numpy as np
import json
import os
from collections import Counter


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Step 1: Get the frequency of each variant
    variants = log_df.groupby(["case:concept:name"])['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    top_20_percent_count = int(len(variants) * 0.2)
    top_variants = variants.nlargest(top_20_percent_count).index.tolist()
    
    # Step 2: Filter log for top 20% variants
    top_cases = log_df[log_df["case:concept:name"].isin(top_variants)]
    
    # Step 3: Discover Petri net from top cases
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(top_cases)
    pm4py.save_vis_petri_net(petri_net, "output/petri_net.png")
    print("OUTPUT_FILE_LOCATION: output/petri_net.png")
    
    # Step 4: Check conformance using token-based replay
    fitness = pm4py.fitness_token_based_replay(top_cases, petri_net, initial_marking, final_marking)
    non_fit_cases = [case for case in top_cases["case:concept:name"].unique() if fitness[case] < 1.0]
    
    # Step 5: Calculate case durations and average duration
    case_durations = top_cases.groupby("case:concept:name")[
        "time:timestamp"].agg(lambda x: (x.max() - x.min()).total_seconds()).reset_index()
    case_durations.columns = ["case:concept:name", "duration"]
    average_duration = case_durations["duration"].mean()
    
    # Step 6: Identify non-fit cases with duration exceeding average
    non_fit_durations = case_durations[case_durations["case:concept:name"].isin(non_fit_cases)]
    delayed_non_fit_cases = non_fit_durations[non_fit_durations["duration"] > average_duration]
    percentage_delayed = (len(delayed_non_fit_cases) / len(non_fit_durations)) * 100 if len(non_fit_durations) > 0 else 0
    
    # Step 7: Identify top 3 resources in delayed non-fit cases
    delayed_cases_df = top_cases[top_cases["case:concept:name"].isin(delayed_non_fit_cases["case:concept:name"])]
    top_resources = Counter(delayed_cases_df["org:resource"]).most_common(3)
    
    # Step 8: Prepare final answer
    final_answer = {
        "percentage_delayed_non_fit_cases": percentage_delayed,
        "top_resources": [resource for resource, _ in top_resources]
    }
    
    # Step 9: Save final answer to JSON
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))