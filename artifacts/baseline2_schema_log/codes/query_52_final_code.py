import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Discover variants and their frequencies
    variants = log_df.groupby("case:concept:name")["concept:name"].apply(lambda x: " -> ".join(x)).value_counts()
    top_20_percent_count = int(len(variants) * 0.2)
    top_variants = variants.nlargest(top_20_percent_count).index.tolist()
    
    # Filter log for top variants
    filtered_log = log_df[log_df.groupby("case:concept:name")["concept:name"].transform(lambda x: " -> ".join(x)).isin(top_variants)]
    
    # Discover Petri net using Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log)
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, "output/petri_net.png")
    print("OUTPUT_FILE_LOCATION: output/petri_net.png")
    
    # Token-based replay to identify non-fit cases
    non_fit_cases = pm4py.conformance_token_based_replay(filtered_log, petri_net, initial_marking, final_marking)
    non_fit_cases_df = pd.DataFrame(non_fit_cases)
    
    # Determine bottleneck activity with longest average sojourn time
    bottleneck_activity = None
    max_sojourn_time = 0
    activity_times = filtered_log.groupby("concept:name").agg({"time:timestamp": ["min", "max"]})
    activity_times.columns = ["start_time", "end_time"]
    activity_times["sojourn_time"] = (activity_times["end_time"] - activity_times["start_time"]).dt.total_seconds()
    
    for activity, row in activity_times.iterrows():
        if row["sojourn_time"] > max_sojourn_time:
            max_sojourn_time = row["sojourn_time"]
            bottleneck_activity = activity
    
    # Report top 3 resources executing the bottleneck activity
    top_resources = filtered_log[filtered_log["concept:name"] == bottleneck_activity]["org:resource"].value_counts().nlargest(3)
    
    # Identify the most frequent variant among cases involving at least one of those resources
    relevant_cases = filtered_log[filtered_log["org:resource"].isin(top_resources.index)]
    relevant_variants = relevant_cases.groupby("case:concept:name")[
        "concept:name"].apply(lambda x: " -> ".join(x)).value_counts()
    most_frequent_variant = relevant_variants.idxmax()
    
    # Prepare final answer
    final_answer = {
        "bottleneck_activity": bottleneck_activity,
        "top_resources": top_resources.index.tolist(),
        "most_frequent_variant": most_frequent_variant
    }
    
    # Save final answer to JSON
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))