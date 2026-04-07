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
    top_variants = variant_counts.groupby("case:concept:name").size().reset_index(name='variant_count')
    top_variants = top_variants.nlargest(int(len(top_variants) * 0.5), 'variant_count')['case:concept:name'].tolist()
    
    # Filter the log for the top 50% variants
    filtered_log = log_df[log_df["case:concept:name"].isin(top_variants)]
    
    # Discover Petri net using Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log)
    
    # Save the Petri net visualization
    petri_net_path = "output/petri_net.png"
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, petri_net_path)
    print(f"OUTPUT_FILE_LOCATION: {petri_net_path}")
    
    # Token-based replay to isolate non-fit cases
    non_fit_cases = pm4py.conformance_token_based_replay(filtered_log, petri_net, initial_marking)
    
    # Get the resources from non-fit cases
    non_fit_resources = [event["org:resource"] for case in non_fit_cases for event in case["trace"]]
    top_resources = Counter(non_fit_resources).most_common(5)
    
    # Prepare final answer
    final_answer = {
        "top_resources": top_resources,
        "petri_net_info": {
            "places": len(petri_net.places),
            "transitions": len(petri_net.transitions),
            "arcs": len(petri_net.arcs),
        }
    }
    
    # Save final answer to JSON
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))