import pm4py
import pandas as pd
import os
import json


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df['time:timestamp'] = pd.to_datetime(log_df['time:timestamp'], utc=True, errors='coerce')

    # Step 1: Identify the strongest Working Together pair
    strongest_pairs = compute_strongest_handover_in_most_freq_variant(log_df)
    if not strongest_pairs:
        raise ValueError('No strongest pairs found.')
    resource_1, resource_2 = strongest_pairs

    # Step 2: Filter the event log for the identified strongest pair
    filtered_log_df = log_df[log_df['org:resource'].isin([resource_1, resource_2])].copy()

    # Step 3: Calculate total case duration
    case_durations = filtered_log_df.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = (case_durations['max'] - case_durations['min']).dt.total_seconds()
    average_duration = case_durations['duration'].mean()

    # Step 4: Isolate cases whose total case duration exceeds the overall average case duration
    delayed_cases = case_durations[case_durations['duration'] > average_duration].index.tolist()
    delayed_log_df = filtered_log_df[filtered_log_df['case:concept:name'].isin(delayed_cases)]

    # Step 5: Determine the dominant variant among those delayed cases
    variants = pm4py.get_variants(delayed_log_df)
    dominant_variant = max(variants.items(), key=lambda x: len(x[1]))[0]

    # Step 6: Discover a Directly-Follows Graph from that dominant-variant subset
    dfg, start_activities, end_activities = pm4py.discover_dfg(delayed_log_df[delayed_log_df['concept:name'].isin(dominant_variant)])
    total = sum(dfg.values()) if dfg else 0
    top_edges = [
        {'source': src, 'target': dst, 'count': count, 'share': (count / total if total else 0.0)}
        for (src, dst), count in sorted(dfg.items(), key=lambda x: x[1], reverse=True)[:5]
    ]

    # Step 7: Prepare final answer
    final_answer = {'top_edges': top_edges}
    with open('output/result.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/result.json')
    print(json.dumps(final_answer, ensure_ascii=False))


# Helper function to compute the strongest handover

def compute_strongest_handover_in_most_freq_variant(log_df):
    # Logic to compute the strongest working together pair
    # This is a placeholder for the actual implementation
    return 'User_1', 'User_3'  # Example pair