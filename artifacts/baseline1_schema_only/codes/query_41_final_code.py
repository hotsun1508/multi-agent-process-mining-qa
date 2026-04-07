import pm4py
import pandas as pd
import json
import os
from collections import Counter


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Step 1: Get the frequency of each variant
    variants = log_df.groupby(['case:concept:name', 'concept:name']).size().reset_index(name='counts')
    variant_counts = variants.groupby('concept:name')['counts'].sum().reset_index()
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count, 'counts')
    top_variant_names = top_variants['concept:name'].tolist()
    
    # Step 2: Filter log for top 20% variants
    filtered_log = log_df[log_df['concept:name'].isin(top_variant_names)]
    
    # Step 3: Discover Petri net from filtered log
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log)
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, "output/petri_net.png")
    print(f"OUTPUT_FILE_LOCATION: output/petri_net.png")
    
    # Step 4: Check conformance and identify non-fit cases
    non_fit_cases = []
    for case_id in filtered_log['case:concept:name'].unique():
        case_log = filtered_log[filtered_log['case:concept:name'] == case_id]
        is_fit = pm4py.conformance_token_based_replay(case_log, petri_net, initial_marking, final_marking)
        if not is_fit:
            non_fit_cases.append(case_id)
    
    # Step 5: Identify resources in non-fit cases
    non_fit_log = filtered_log[filtered_log['case:concept:name'].isin(non_fit_cases)]
    top_resources = Counter(non_fit_log['org:resource']).most_common(5)
    top_resources_dict = {resource: count for resource, count in top_resources}
    
    # Step 6: Prepare final answer
    final_answer = {
        "behavior_variant": top_variant_names,
        "process_discovery": "output/petri_net.png",
        "conformance": non_fit_cases,
        "resource": top_resources_dict
    }
    
    # Save final answer as JSON
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))