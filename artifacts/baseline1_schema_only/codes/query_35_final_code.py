import pm4py
import pandas as pd
import json
import os
from collections import Counter


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    # Calculate sojourn time for each activity
    log_df['sojourn_time'] = log_df.groupby(['case:concept:name', 'concept:name'])['time:timestamp'].transform(lambda x: x.max() - x.min()).dt.total_seconds() / 3600  # Convert to hours
    avg_sojourn_time = log_df.groupby('concept:name')['sojourn_time'].mean().sort_values(ascending=False)
    bottleneck_activity = avg_sojourn_time.idxmax()
    bottleneck_avg_time = avg_sojourn_time.max()

    # Find top 5 resources executing the bottleneck activity
    top_resources = log_df[log_df['concept:name'] == bottleneck_activity]['org:resource'].value_counts().head(5).index.tolist()

    # Filter cases containing the bottleneck activity and at least one of the top resources
    filtered_cases = log_df[(log_df['concept:name'] == bottleneck_activity) | (log_df['org:resource'].isin(top_resources))]
    case_ids = filtered_cases['case:concept:name'].unique()
    filtered_log = log_df[log_df['case:concept:name'].isin(case_ids)]

    # Find the dominant variant
    variants = filtered_log.groupby(['case:concept:name', 'concept:name']).size().reset_index(name='count')
    dominant_variant = variants.groupby('concept:name')['count'].sum().idxmax()

    # Prepare final answer
    final_answer = {
        'bottleneck_activity': bottleneck_activity,
        'bottleneck_avg_time': bottleneck_avg_time,
        'top_resources': top_resources,
        'dominant_variant': dominant_variant
    }

    # Save final answer to JSON
    output_path = 'output/final_answer.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  

    print(json.dumps(final_answer, ensure_ascii=False))