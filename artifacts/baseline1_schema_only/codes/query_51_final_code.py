import pm4py
import pandas as pd
import numpy as np
import json
import os
from collections import Counter


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Step 1: Identify the top 20% most frequent variants
    variant_counts = log_df["case:concept:name"].value_counts()
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count).index
    top_cases = log_df[log_df["case:concept:name"].isin(top_variants)]
    
    # Step 2: Discover a reference Petri net from the top 20% variants
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(top_cases)
    pm4py.save_vis_petri_net(petri_net, "output/reference_petri_net.png")
    print("OUTPUT_FILE_LOCATION: output/reference_petri_net.png")
    
    # Step 3: Identify non-fit cases under token-based replay
    fitness = pm4py.conformance_token_based_replay(top_cases, petri_net, initial_marking, final_marking)
    non_fit_cases = top_cases[~fitness["fit"]]
    
    # Step 4: Calculate the percentage of non-fit cases exceeding average case duration
    average_duration = (log_df.groupby("case:concept:name")["time:timestamp"].max() - log_df.groupby("case:concept:name")["time:timestamp"].min()).mean().total_seconds()
    non_fit_cases_duration = (non_fit_cases.groupby("case:concept:name")["time:timestamp"].max() - non_fit_cases.groupby("case:concept:name")["time:timestamp"].min()).dt.total_seconds()
    percentage_exceeding_average = (non_fit_cases_duration[non_fit_cases_duration > average_duration].count() / non_fit_cases_duration.count()) * 100
    
    # Step 5: Identify top 3 resources in non-fit cases
    top_resources = Counter(non_fit_cases["org:resource"]).most_common(3)
    top_resources_list = [resource for resource, count in top_resources]
    
    # Final answer
    final_answer = {
        "percentage_exceeding_average": percentage_exceeding_average,
        "top_resources": top_resources_list,
    }
    
    # Save final answer
    with open("output/final_benchmark_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_benchmark_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))