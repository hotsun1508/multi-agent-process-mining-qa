import pm4py
import json
import pandas as pd


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for packages
    flat_log = pm4py.ocel_flattening(ocel, 'packages')
    # Discover the DFG
    dfg, start_activities, end_activities = pm4py.discover_dfg(flat_log)
    # Find the most frequent DFG edge
    total = sum(dfg.values())
    most_frequent_edge = max(dfg.items(), key=lambda x: x[1])
    source, target = most_frequent_edge[0]
    edge_count = most_frequent_edge[1]
    # Filter cases containing the most frequent edge
    filtered_cases = flat_log[flat_log['concept:name'].shift() == source]
    filtered_cases = filtered_cases[filtered_cases['concept:name'] == target]
    # Calculate the most dominant variant
    variant_counts = filtered_cases['case:concept:name'].value_counts()
    most_dominant_variant = variant_counts.idxmax()
    # Calculate average case duration
    case_durations = filtered_cases.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = case_durations['max'] - case_durations['min']
    average_duration = case_durations['duration'].mean().total_seconds()
    # Save edge subset stats
    edge_subset_stats = {
        'most_frequent_edge': {'source': source, 'target': target, 'count': edge_count},
        'most_dominant_variant': most_dominant_variant,
        'average_case_duration': average_duration
    }
    with open('output/edge_subset_stats.json', 'w', encoding='utf-8') as f:
        json.dump(edge_subset_stats, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/edge_subset_stats.json')
    # Prepare final answer
    final_answer = {
        'process_discovery': {'most_frequent_edge': edge_subset_stats['most_frequent_edge']},
        'behavior_variant': {'most_dominant_variant': edge_subset_stats['most_dominant_variant']},
        'performance': {'average_case_duration': edge_subset_stats['average_case_duration']}
    }
    print(json.dumps(final_answer, ensure_ascii=False))