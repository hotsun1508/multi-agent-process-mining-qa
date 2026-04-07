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
    variant_counts = filtered_log_df.groupby("case:concept:name")[
        "concept:name"].apply(lambda x: " -> ".join(x)).value_counts()
    dominant_variant = variant_counts.idxmax()
    print(f"Dominant variant: {dominant_variant}")

    # Step 4: Keep only the delayed cases in that dominant variant whose total case duration exceeds the overall average case duration
    case_durations = filtered_log_df.groupby("case:concept:name").agg(total_duration=("time:timestamp", lambda x: (x.max() - x.min()).total_seconds()))
    overall_avg_duration = case_durations["total_duration"].mean()
    delayed_cases = case_durations[case_durations["total_duration"] > overall_avg_duration].index.tolist()
    delayed_log_df = filtered_log_df[filtered_log_df["case:concept:name"].isin(delayed_cases)]

    # Step 5: Discover a Petri net from those delayed cases
    petri_net = pm4py.discover_petri_net_inductive(pm4py.convert_to_event_log(delayed_log_df))
    petri_net_path = "output/petri_net.png"
    pm4py.save_vis_petri_net(petri_net, petri_net_path)
    print(f"OUTPUT_FILE_LOCATION: {petri_net_path}")

    # Step 6: Report how many cases are fit under token-based replay
    token_based_replay_fit = pm4py.check_token_based_replay(petri_net, delayed_log_df)
    print(f"Token-based replay fit cases: {len(token_based_replay_fit)}")

    # Final answer
    final_answer = {
        "strongest_pair": strongest_pair,
        "dominant_variant": dominant_variant,
        "delayed_cases_count": len(delayed_cases),
        "token_based_replay_fit": len(token_based_replay_fit)
    }
    print(json.dumps(final_answer, ensure_ascii=False))