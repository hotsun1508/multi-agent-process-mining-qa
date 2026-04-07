import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    # Calculate throughput time for each case
    log_df['throughput_time'] = log_df.groupby('case:concept:name')['time:timestamp'].transform(lambda x: (x.max() - x.min()).total_seconds())
    # Get the frequency of each variant
    variant_counts = log_df.groupby('concept:name')['case:concept:name'].nunique().reset_index(name='case_count')
    # Get the top 1% most frequent variants
    top_1_percent_threshold = variant_counts['case_count'].quantile(0.99)
    top_variants = variant_counts[variant_counts['case_count'] >= top_1_percent_threshold]
    # Calculate median throughput time for each top variant
    median_throughput = log_df[log_df['concept:name'].isin(top_variants['concept:name'])].groupby('concept:name')['throughput_time'].median().reset_index()
    # Identify the variant with the highest median throughput time
    highest_median_variant = median_throughput.loc[median_throughput['throughput_time'].idxmax()]
    variant_name = highest_median_variant['concept:name']
    # Get the top 3 resources for that variant
    top_resources = log_df[log_df['concept:name'] == variant_name]['org:resource'].value_counts().head(3).index.tolist()
    # Prepare final answer
    final_answer = {
        'behavior_variant': variant_name,
        'performance': highest_median_variant['throughput_time'],
        'resource': top_resources
    }
    # Save final answer to JSON file
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    print(json.dumps(final_answer, ensure_ascii=False))