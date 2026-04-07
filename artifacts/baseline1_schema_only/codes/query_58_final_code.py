import pm4py
import pandas as pd
import json
import os
import statistics


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Calculate case durations and overall average duration
    log_df["case_duration"] = log_df.groupby("case:concept:name")["time:timestamp"].transform(lambda x: (x.max() - x.min()).total_seconds())
    overall_avg_duration = log_df["case_duration"].mean()
    
    # Get the top 20% most frequent variants
    variant_counts = log_df["case:concept:name"].value_counts()
    top_variants = variant_counts.head(int(len(variant_counts) * 0.2)).index.tolist()
    
    # Filter log for top variants
    top_variant_log = log_df[log_df["case:concept:name"].isin(top_variants)]
    
    # Keep only delayed cases with duration exceeding the average
    delayed_cases = top_variant_log[top_variant_log["case_duration"] > overall_avg_duration]
    
    # Discover Petri net from delayed cases
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(delayed_cases)
    pn_path = "output/reference_petri_net.png"
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, pn_path)
    print(f"OUTPUT_FILE_LOCATION: {pn_path}")
    
    # Token-based replay to find non-fit cases
    replay_results = pm4py.algo.conformance.token_based_replay.apply(delayed_cases, petri_net, initial_marking)
    non_fit_cases = delayed_cases[replay_results["fit"] == False]
    
    # Identify top 3 resources in non-fit cases
    top_resources = non_fit_cases["org:resource"].value_counts().head(3).to_dict()
    
    # Prepare final answer
    final_answer = {
        "behavior_variant": top_variants,
        "performance": overall_avg_duration,
        "process_discovery": {
            "petri_net": pn_path
        },
        "conformance": {
            "non_fit_cases": non_fit_cases["case:concept:name"].unique().tolist(),
            "top_resources": top_resources
        },
        "resource": top_resources
    }
    
    # Save final answer to JSON
    with open("output/final_benchmark_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_benchmark_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))