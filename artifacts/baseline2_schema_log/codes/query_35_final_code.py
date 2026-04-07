import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    # Calculate sojourn time for each activity
    log_df['time:timestamp'] = pd.to_datetime(log_df['time:timestamp'])
    log_df['next_time'] = log_df.groupby('case:concept:name')['time:timestamp'].shift(-1)
    log_df['sojourn_time'] = (log_df['next_time'] - log_df['time:timestamp']).dt.total_seconds() / 60  # in minutes
    sojourn_time_avg = log_df.groupby('concept:name')['sojourn_time'].mean().sort_values(ascending=False)
    bottleneck_activity = sojourn_time_avg.idxmax()
    # Find top 5 resources for the bottleneck activity
    top_resources = log_df[log_df['concept:name'] == bottleneck_activity]['org:resource'].value_counts().head(5).index.tolist()
    # Filter cases containing the bottleneck activity and at least one of the top resources
    filtered_cases = log_df[(log_df['concept:name'] == bottleneck_activity) | (log_df['org:resource'].isin(top_resources))]
    dominant_variant = filtered_cases.groupby('case:concept:name')['concept:name'].apply(lambda x: x.tolist()).value_counts().idxmax()
    # Prepare final answer
    final_answer = {
        'bottleneck_activity': bottleneck_activity,
        'top_resources': top_resources,
        'dominant_variant': dominant_variant
    }
    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    print(json.dumps(final_answer, ensure_ascii=False))