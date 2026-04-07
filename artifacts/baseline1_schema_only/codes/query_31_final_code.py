import pm4py
import pandas as pd
import json
import os
import statistics

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Get the frequency of each variant
    variant_counts = log_df.groupby(["case:concept:name", "concept:name"]).size().reset_index(name='counts')
    top_variants = variant_counts.groupby("case:concept:name").sum().nlargest(int(len(variant_counts) * 0.2), "counts")
    top_variant_cases = top_variants.index.tolist()
    
    # Filter the log for the top variants
    filtered_log = log_df[log_df["case:concept:name"].isin(top_variant_cases)]
    
    # Discover the Petri net using the Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log)
    
    # Save the Petri net visualization
    petri_net_path = "output/petri_net.png"
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, petri_net_path)
    print(f"OUTPUT_FILE_LOCATION: {petri_net_path}")
    
    # Calculate throughput time for the filtered log
    filtered_log['time:timestamp'] = pd.to_datetime(filtered_log['time:timestamp'])
    throughput_times = filtered_log.groupby("case:concept:name").agg(
        start_time=('time:timestamp', 'min'),
        end_time=('time:timestamp', 'max')
    )
    throughput_times['throughput_time'] = (throughput_times['end_time'] - throughput_times['start_time']).dt.total_seconds() / 3600  # in hours
    average_throughput_time = statistics.mean(throughput_times['throughput_time'])
    
    # Prepare final answer
    final_answer = {
        "average_throughput_time": average_throughput_time,
        "top_variant_cases_count": len(top_variant_cases)
    }
    
    # Save final answer to JSON
    with open("output/final_answer.json", "w", encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))