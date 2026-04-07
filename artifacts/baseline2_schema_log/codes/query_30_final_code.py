import pm4py
import pandas as pd
import json
import os
from collections import Counter


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Calculate Working Together metric
    resource_pairs = log_df.groupby(["case:concept:name", "org:resource"])['concept:name'].count().reset_index()
    resource_pairs.columns = ["case_id", "resource", "count"]
    working_together = resource_pairs.groupby("case_id")["resource"].apply(lambda x: list(x)).reset_index()
    working_together['resource_count'] = working_together['resource'].apply(len)
    
    # Count resource collaborations
    collaborations = Counter()
    for resources in working_together['resource']:
        if len(resources) > 1:
            for i in range(len(resources)):
                for j in range(i + 1, len(resources)):
                    collaborations[(resources[i], resources[j])] += 1
    
    # Identify top 3 collaborating resources
    top_resources = collaborations.most_common(3)
    top_resource_names = set()
    for (res1, res2), count in top_resources:
        top_resource_names.add(res1)
        top_resource_names.add(res2)
    
    # Filter cases involving all three resources
    filtered_cases = log_df[log_df['org:resource'].isin(top_resource_names)]
    case_counts = filtered_cases.groupby("case:concept:name").size().reset_index(name='counts')
    dominant_variant = case_counts['case:concept:name'].mode()[0]
    
    # Prepare final answer
    final_answer = {
        "top_resources": list(top_resource_names),
        "dominant_variant": dominant_variant
    }
    
    # Save final answer
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))