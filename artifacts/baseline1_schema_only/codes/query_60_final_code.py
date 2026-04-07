import pm4py
import pandas as pd
import numpy as np
import json
import os
from collections import Counter


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])

    # Step 1: Identify the strongest working Together pair of resources
    resource_pairs = Counter()
    for case_id, group in log_df.groupby("case:concept:name"):
        resources = group["org:resource"].tolist()
        unique_resources = set(resources)
        if len(unique_resources) > 1:
            for r1 in unique_resources:
                for r2 in unique_resources:
                    if r1 != r2:
                        resource_pairs[(r1, r2)] += 1

    strongest_pair = resource_pairs.most_common(1)[0][0]
    print(f"Strongest resource pair: {strongest_pair}")

    # Step 2: Select cases containing that pair
    filtered_cases = log_df[(log_df["org:resource"] == strongest_pair[0]) | (log_df["org:resource"] == strongest_pair[1])]
    filtered_case_ids = filtered_cases["case:concept:name"].unique()
    filtered_log_df = log_df[log_df["case:concept:name"].isin(filtered_case_ids)]

    # Step 3: Determine the dominant variant in that subset
    variant_counts = filtered_log_df["concept:name"].value_counts()
    dominant_variant = variant_counts.idxmax()
    print(f"Dominant variant: {dominant_variant}")

    # Step 4: Keep only delayed cases in that dominant variant whose total case duration exceeds the overall average case duration
    case_durations = filtered_log_df.groupby("case:concept:name").agg({"time:timestamp": ["min", "max"]})
    case_durations.columns = ["start_time", "end_time"]
    case_durations["duration"] = (case_durations["end_time"] - case_durations["start_time"]).dt.total_seconds() / 60
    overall_avg_duration = case_durations["duration"].mean()
    delayed_cases = case_durations[case_durations["duration"] > overall_avg_duration].index.tolist()
    delayed_log_df = filtered_log_df[filtered_log_df["case:concept:name"].isin(delayed_cases)]

    # Step 5: Discover a Petri net from those delayed cases
    petri_net = pm4py.discover_petri_net_inductive(pm4py.convert_to_event_log(delayed_log_df))
    png_path = "output/petri_net.png"
    pm4py.save_vis_petri_net(petri_net, png_path)
    print(f"OUTPUT_FILE_LOCATION: {png_path}")

    # Step 6: Report how many cases are fit under token-based replay
    token_based_replay_fit = pm4py.conformance_token_based_replay(event_log, petri_net)
    fit_cases_count = len(token_based_replay_fit["fit_cases"])

    # Final answer
    final_answer = {
        "resource": strongest_pair,
        "behavior_variant": dominant_variant,
        "performance": {
            "overall_avg_duration": overall_avg_duration,
            "delayed_cases_count": len(delayed_cases),
            "fit_cases_count": fit_cases_count
        },
        "process_discovery": "Petri net discovered and saved.",
        "conformance": fit_cases_count
    }
    print(json.dumps(final_answer, ensure_ascii=False))