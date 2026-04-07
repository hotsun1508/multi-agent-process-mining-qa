import pm4py
import pandas as pd
import json
import os
import statistics


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Step 1: Identify the top 5 resources by event frequency
    top_resources = log_df["org:resource"].value_counts().head(5).index.tolist()
    
    # Step 2: Select cases involving at least one of the top resources
    filtered_cases = log_df[log_df["org:resource"].isin(top_resources)]
    
    # Step 3: Determine the dominant variant in that subset
    variant_counts = filtered_cases.groupby(["case:concept:name", "concept:name"]).size().reset_index(name='count')
    dominant_variant = variant_counts.groupby(["case:concept:name"])['count'].sum().idxmax()
    
    # Step 4: Discover a Petri net from the cases of that variant
    variant_cases = filtered_cases[filtered_cases["case:concept:name"] == dominant_variant]
    petri_net = pm4py.discover_petri_net_inductive(variant_cases)
    petri_net_path = "output/petri_net.png"
    pm4py.save_vis_petri_net(petri_net, petri_net_path)
    print(f"OUTPUT_FILE_LOCATION: {petri_net_path}")
    
    # Step 5: Identify non-fit cases under token-based replay
    fitness = pm4py.evaluate_token_based_fitness(variant_cases, petri_net)
    non_fit_cases = variant_cases[fitness['fit'] == False]
    average_throughput_time = statistics.mean(non_fit_cases["time:timestamp"].diff().dt.total_seconds())
    
    # Prepare final answer
    final_answer = {
        "resource": top_resources,
        "behavior_variant": dominant_variant,
        "process_discovery": petri_net_path,
        "conformance": len(non_fit_cases),
        "performance": average_throughput_time
    }
    
    # Save final answer to JSON
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))