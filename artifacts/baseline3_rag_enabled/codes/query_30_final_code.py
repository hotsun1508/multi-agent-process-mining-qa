import pm4py
import pandas as pd
import os
from collections import Counter

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])

    # Step 1: Compute the co-occurrence of resources for each case
    resource_pairs = log_df.groupby('case:concept:name')['org:resource'].apply(lambda x: list(set(x))).tolist()
    resource_counts = Counter()

    for resources in resource_pairs:
        if len(resources) > 1:
            for i in range(len(resources)):
                for j in range(i + 1, len(resources)):
                    resource_counts[(resources[i], resources[j])] += 1

    # Step 2: Identify top 3 collaborating resources based on the Working Together metric
    resource_collaboration = Counter()
    for (res1, res2), count in resource_counts.items():
        resource_collaboration[res1] += count
        resource_collaboration[res2] += count

    top_resources = resource_collaboration.most_common(3)
    top_resource_names = [res[0] for res in top_resources]

    # Step 3: Filter cases involving the top 3 resources
    filtered_cases = log_df[log_df['org:resource'].isin(top_resource_names)]
    case_variants = filtered_cases.groupby('case:concept:name')['concept:name'].apply(list)
    variant_counts = Counter(tuple(variant) for variant in case_variants)

    # Step 4: Identify the dominant variant
    dominant_variant = variant_counts.most_common(1)[0][0] if variant_counts else None

    # Step 5: Prepare final answer
    final_answer = {
        'top_resources': top_resource_names,
        'dominant_variant': dominant_variant,
        'result_type': 'composite',
        'view': 'event_log',
        'result_schema': {'resource': 'table', 'behavior_variant': 'metric_dict'},
        'artifacts_schema': ['output/*']
    }

    # Step 6: Save results
    with open('output/result.json', 'w') as f:
        json.dump(final_answer, f, ensure_ascii=False)
    print('OUTPUT_FILE_LOCATION: output/result.json')

    # Optional: Save the filtered cases to CSV
    filtered_cases.to_csv('output/filtered_cases.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/filtered_cases.csv')

    print(json.dumps(final_answer, ensure_ascii=False))