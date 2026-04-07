import pm4py
import pandas as pd
import json
import os
from pm4py.objects.log.util import dataframe_utils
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.algo.discovery.variants import inductive as inductive_discovery
from pm4py.algo.conformance.tokenreplay import algorithm as token_replay


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    dfg, start_activities, end_activities = dfg_discovery.apply(event_log)
    
    # Find the most frequent edge
    total = sum(dfg.values()) if dfg else 0
    most_frequent_edge = max(dfg.items(), key=lambda x: x[1])
    most_frequent_edge_count = most_frequent_edge[1]
    most_frequent_edge_key = most_frequent_edge[0]
    
    # Filter cases containing the most frequent edge
    source_activity, target_activity = most_frequent_edge_key
    filtered_cases = log_df[(log_df["concept:name"] == source_activity) | (log_df["concept:name"] == target_activity)]
    filtered_case_ids = filtered_cases["case:concept:name"].unique()
    filtered_log = event_log[filtered_log["case:concept:name"].isin(filtered_case_ids)]
    
    # Determine the dominant variant within the filtered subset
    variant_counts = filtered_log.groupby(["case:concept:name"])['concept:name'].apply(lambda x: '->'.join(x)).value_counts()
    dominant_variant = variant_counts.idxmax()
    
    # Use the reference Petri net for token-based replay
    ocpn = pm4py.discover_petri_net_inductive(filtered_log)
    non_fit_cases = token_replay.apply(filtered_log, ocpn)
    non_fit_case_ids = [case for case, fit in non_fit_cases.items() if not fit]
    
    # Calculate average throughput time of non-fit cases
    non_fit_df = log_df[log_df["case:concept:name"].isin(non_fit_case_ids)]
    non_fit_df["time:timestamp"] = pd.to_datetime(non_fit_df["time:timestamp"])
    throughput_times = non_fit_df.groupby("case:concept:name").agg(lambda x: (x.max() - x.min()).total_seconds()).mean()
    average_throughput_time = throughput_times.mean()
    
    # Identify top 3 resources in non-fit cases
    top_resources = non_fit_df["org:resource"].value_counts().head(3).to_dict()
    
    # Prepare final answer
    final_answer = {
        "most_frequent_edge": str(most_frequent_edge_key),
        "dominant_variant": dominant_variant,
        "average_throughput_time": average_throughput_time,
        "top_resources": top_resources
    }
    
    # Save results
    with open("output/final_benchmark_result.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_benchmark_result.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))