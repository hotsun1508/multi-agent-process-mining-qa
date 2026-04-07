import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Discover the Petri net
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(event_log)
    
    # Get the top 20% most frequent variants
    variant_counts = log_df.groupby(["case:concept:name"])['concept:name'].apply(lambda x: list(x)).value_counts()
    top_variants = variant_counts.head(int(len(variant_counts) * 0.2)).index.tolist()
    
    # Filter cases belonging to the top variants
    top_cases = log_df[log_df["case:concept:name"].isin(top_variants)]
    
    # Token-based replay to find non-fit cases
    non_fit_cases = []
    for case_id in top_cases["case:concept:name"].unique():
        case_events = top_cases[top_cases["case:concept:name"] == case_id]
        fit, _ = pm4py.token_based_replay(case_events, petri_net, initial_marking, final_marking)
        if not fit:
            non_fit_cases.append(case_id)
    
    # Analyze sojourn times for non-fit cases
    non_fit_df = log_df[log_df["case:concept:name"].isin(non_fit_cases)]
    sojourn_times = non_fit_df.groupby(["case:concept:name", "concept:name"]).agg({"time:timestamp": ["min", "max"]})
    sojourn_times.columns = ["start_time", "end_time"]
    sojourn_times["duration"] = (sojourn_times["end_time"] - sojourn_times["start_time"]).dt.total_seconds()
    average_sojourn = sojourn_times.groupby("concept:name")["duration"].mean().reset_index()
    longest_activity = average_sojourn.loc[average_sojourn["duration"].idxmax()]
    
    # Get top 3 resources for the longest activity
    top_resources = non_fit_df[non_fit_df["concept:name"] == longest_activity["concept:name"]]["org:resource"].value_counts().head(3)
    
    # Prepare final answer
    final_answer = {
        "longest_activity": longest_activity["concept:name"],
        "average_duration": longest_activity["duration"],
        "top_resources": top_resources.index.tolist(),
    }
    
    # Save final answer to JSON
    output_path = "output/final_answer.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: {output_path}")
    
    # Print final answer
    print(json.dumps(final_answer, ensure_ascii=False))