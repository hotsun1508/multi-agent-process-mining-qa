import pm4py
import pandas as pd
import json
import os
import statistics
from collections import Counter

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Calculate case durations
    log_df["case_duration"] = log_df.groupby("case:concept:name")["time:timestamp"].transform(lambda x: (x.max() - x.min()).total_seconds())
    average_case_duration = log_df["case_duration"].mean()
    
    # Get the top 20% most frequent variants
    variants = log_df.groupby(["case:concept:name", "concept:name"]).size().reset_index(name='counts')
    top_variants = variants.nlargest(int(len(variants) * 0.2), 'counts')
    top_variant_cases = log_df[log_df["case:concept:name"].isin(top_variants["case:concept:name"])]
    
    # Filter delayed cases with duration exceeding average
    delayed_cases = top_variant_cases[top_variant_cases["case_duration"] > average_case_duration]
    
    # Discover Petri net from delayed cases
    petri_net = pm4py.discover_petri_net_inductive(delayed_cases)
    pm4py.save_vis_petri_net(petri_net, "output/petri_net.png")
    print("OUTPUT_FILE_LOCATION: output/petri_net.png")
    
    # Token-based replay to find non-fit cases
    non_fit_cases = pm4py.conformance_token_based_replay(delayed_cases, petri_net)
    
    # Identify top 3 resources in non-fit cases
    non_fit_case_ids = [case[0] for case in non_fit_cases if case[1] == False]
    non_fit_resources = delayed_cases[delayed_cases["case:concept:name"].isin(non_fit_case_ids)]["org:resource"].value_counts().head(3)
    top_resources = non_fit_resources.index.tolist()
    
    # Prepare final answer
    final_answer = {
        "top_resources": top_resources,
        "average_case_duration": average_case_duration,
        "total_delayed_cases": len(delayed_cases),
        "total_non_fit_cases": len(non_fit_case_ids)
    }
    
    # Save final answer to JSON
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))