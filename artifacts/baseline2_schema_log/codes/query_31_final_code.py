import pm4py
import pandas as pd
import json
import os
import statistics

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Get the frequency of each variant
    variants = log_df.groupby(["case:concept:name", "concept:name"]).size().reset_index(name='count')
    top_variants = variants.groupby("case:concept:name").sum().nlargest(int(len(variants) * 0.2), "count")
    top_variant_cases = top_variants.index.tolist()
    
    # Filter the log for the top 20% variants
    filtered_log = log_df[log_df["case:concept:name"].isin(top_variant_cases)]
    
    # Discover the Petri net using the Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log)
    
    # Save the Petri net visualization
    petri_net_path = "output/petri_net.png"
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, petri_net_path)
    print(f"OUTPUT_FILE_LOCATION: {petri_net_path}")
    
    # Calculate throughput time for non-fit cases
    throughput_times = []
    for case in top_variant_cases:
        case_log = filtered_log[filtered_log["case:concept:name"] == case]
        if not case_log.empty:
            start_time = case_log["time:timestamp"].min()
            end_time = case_log["time:timestamp"].max()
            throughput_time = (end_time - start_time).total_seconds()
            throughput_times.append(throughput_time)
    
    # Calculate average throughput time
    average_throughput_time = statistics.mean(throughput_times) if throughput_times else 0
    
    # Prepare final answer
    final_answer = {
        "average_throughput_time": average_throughput_time,
        "petri_net_variants": len(top_variant_cases)
    }
    
    # Save final answer to JSON
    with open("output/final_benchmark_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: output/final_benchmark_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))