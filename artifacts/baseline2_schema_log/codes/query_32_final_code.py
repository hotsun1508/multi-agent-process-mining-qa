import pm4py
import pandas as pd
import json
import os
from collections import Counter

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    # Get the frequency of each variant
    variant_counts = log_df.groupby(["case:concept:name", "concept:name"]).size().reset_index(name='count')
    top_variants = variant_counts.groupby("case:concept:name").sum().nlargest(int(len(variant_counts) * 0.5), "count").index.tolist()
    filtered_log = log_df[log_df["case:concept:name"].isin(top_variants)]
    
    # Discover Petri net using Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log)
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, "output/petri_net.png")
    print("OUTPUT_FILE_LOCATION: output/petri_net.png")
    
    # Token-based replay to isolate non-fit cases
    non_fit_cases = []
    for case_id in filtered_log["case:concept:name"].unique():
        case_log = filtered_log[filtered_log["case:concept:name"] == case_id]
        is_fit = pm4py.token_based_replay(case_log, petri_net, initial_marking, final_marking)
        if not is_fit:
            non_fit_cases.append(case_id)
    
    # Get resources from non-fit cases
    non_fit_log = filtered_log[filtered_log["case:concept:name"].isin(non_fit_cases)]
    top_resources = non_fit_log["org:resource"].value_counts().head(5).to_dict()
    
    # Prepare final answer
    final_answer = {
        "top_resources": top_resources,
        "non_fit_cases_count": len(non_fit_cases)
    }
    
    # Save final answer to JSON
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))