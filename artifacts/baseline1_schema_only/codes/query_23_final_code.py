import pm4py
import pandas as pd
import json
from collections import defaultdict


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Identify the most frequent variant
    variant_counts = log_df["concept:name"].value_counts()
    most_frequent_variant = variant_counts.idxmax()
    
    # Filter log for the most frequent variant
    filtered_log = log_df[log_df["concept:name"] == most_frequent_variant]
    
    # Analyze handover of work between resource pairs
    handover_counts = defaultdict(int)
    for case_id, group in filtered_log.groupby("case:concept:name"):
        resources = group["org:resource"].tolist()
        for i in range(len(resources) - 1):
            handover_counts[(resources[i], resources[i + 1])] += 1
    
    # Find the resource pair with the strongest handover of work relation
    strongest_handover = max(handover_counts.items(), key=lambda x: x[1])
    resource_pair, count = strongest_handover
    
    # Prepare final answer
    final_answer = {
        "resource_pair": resource_pair,
        "handover_count": count,
        "most_frequent_variant": most_frequent_variant
    }
    
    # Save final answer to JSON
    with open("output/strongest_handover.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/strongest_handover.json")
    
    # Print final answer
    print(json.dumps(final_answer, ensure_ascii=False))