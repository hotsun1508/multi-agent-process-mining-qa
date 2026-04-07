import os
import json
import pandas as pd
import pm4py
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.statistics.variants.log import get as variants_get


def main():
    event_log = ACTIVE_LOG
    os.makedirs('output', exist_ok=True)

    # Step 1: Convert the event log to a DataFrame
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])

    # Step 2: Calculate sojourn time for each activity
    log_df['next_timestamp'] = log_df.groupby('case:concept:name')['time:timestamp'].shift(-1)
    log_df['sojourn_time'] = (log_df['next_timestamp'] - log_df['time:timestamp']).dt.total_seconds()

    # Step 3: Identify the activity with the longest average sojourn time
    sojourn_times = log_df.groupby('concept:name')['sojourn_time'].mean().dropna()
    longest_avg_activity = sojourn_times.idxmax()
    longest_avg_time = sojourn_times.max()

    # Step 4: Filter the dataframe to include only events related to the identified activity
    activity_df = log_df[log_df['concept:name'] == longest_avg_activity]

    # Step 5: Find the top 5 resources executing the identified activity most frequently
    top_resources = activity_df['org:resource'].value_counts().head(5).index.tolist()

    # Step 6: Filter cases that contain both the bottleneck activity and at least one of those resources
    filtered_cases = log_df[(log_df['concept:name'] == longest_avg_activity) & (log_df['org:resource'].isin(top_resources))]
    case_ids = filtered_cases['case:concept:name'].unique()
    filtered_log = log_df[log_df['case:concept:name'].isin(case_ids)]

    # Step 7: Get the dominant variant among the filtered cases
    variants = variants_get.get_variants(filtered_log)
    dominant_variant = max(variants.items(), key=lambda x: len(x[1]))[0]

    # Step 8: Prepare the final result dictionary
    result = {
        'activity_with_longest_sojourn_time': longest_avg_activity,
        'average_sojourn_time': longest_avg_time,
        'top_resources': top_resources,
        'dominant_variant': dominant_variant
    }

    # Save the result to a JSON file
    with open('output/result.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/result.json')

    print(json.dumps(result, ensure_ascii=False))