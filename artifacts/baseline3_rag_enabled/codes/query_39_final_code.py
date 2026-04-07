import os
import json
import pm4py
import pandas as pd
import numpy as np


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])

    # Calculate throughput time
    log_df['throughput_time'] = log_df.groupby('case:concept:name')['time:timestamp'].transform(lambda x: (x.max() - x.min()).total_seconds())

    # Get variants and their frequencies
    variants = log_df.groupby(['case:concept:name', 'concept:name']).size().reset_index(name='frequency')
    top_variants = variants.groupby('case:concept:name').agg({'frequency': 'sum'}).nlargest(int(len(variants) * 0.01), 'frequency').reset_index()

    # Calculate median throughput time for top variants
    median_throughput = log_df[log_df['case:concept:name'].isin(top_variants['case:concept:name'])].groupby('case:concept:name')['throughput_time'].median().reset_index()
    median_throughput = median_throughput.nlargest(1, 'throughput_time')

    # Get the variant with the highest median throughput time
    variant_name = median_throughput['case:concept:name'].values[0]

    # Extract top 3 resources for that variant
    top_resources = log_df[log_df['case:concept:name'] == variant_name]['org:resource'].value_counts().nlargest(3).index.tolist()

    # Prepare final answer
    final_answer = {
        'behavior_variant': variant_name,
        'performance': median_throughput['throughput_time'].values[0],
        'resource': top_resources
    }

    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')

    print(json.dumps(final_answer, ensure_ascii=False))