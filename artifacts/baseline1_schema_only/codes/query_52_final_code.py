import pm4py
import pandas as pd
import json
import os
import statistics
from collections import Counter

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Discover variants and get the top 20% most frequent variants
    variant_counts = log_df.groupby("case:concept:name").size().reset_index(name='counts')
    top_variants = variant_counts.nlargest(int(len(variant_counts) * 0.2), 'counts')
    top_variant_cases = log_df[log_df["case:concept:name"].isin(top_variants["case:concept:name"])]
    
    # Discover Petri net using Inductive Miner
    net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(top_variant_cases)
    png_path = "output/petri_net.png"
    pm4py.save_vis_petri_net(net, initial_marking, final_marking, png_path)
    print(f"OUTPUT_FILE_LOCATION: {png_path}")
    
    # Token-based replay to identify non-fit cases
    non_fit_cases = pm4py.replay_log(log=top_variant_cases, net=net, initial_marking=initial_marking, final_marking=final_marking, variant=pm4py.replay.variants.TOKEN_BASED)
    non_fit_cases_df = pd.DataFrame(non_fit_cases)
    
    # Determine bottleneck activity with longest average sojourn time
    activity_durations = non_fit_cases_df.groupby("concept:name")["time:timestamp"].agg(["min", "max"])
    activity_durations["duration"] = activity_durations["max"] - activity_durations["min"]
    bottleneck_activity = activity_durations["duration"].idxmax()
    
    # Get top 3 resources executing the bottleneck activity
    resources_executing_bottleneck = non_fit_cases_df[non_fit_cases_df["concept:name"] == bottleneck_activity]["org:resource"].value_counts().nlargest(3)
    
    # Identify the most frequent variant among cases involving at least one of those resources
    relevant_cases = non_fit_cases_df[non_fit_cases_df["org:resource"].isin(resources_executing_bottleneck.index)]
    most_frequent_variant = relevant_cases["case:concept:name"].value_counts().idxmax()
    
    # Prepare final answer
    final_answer = {
        "bottleneck_activity": bottleneck_activity,
        "top_resources": resources_executing_bottleneck.index.tolist(),
        "most_frequent_variant": most_frequent_variant
    }
    
    # Save final answer to JSON
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))