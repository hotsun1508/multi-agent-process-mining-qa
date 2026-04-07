import os
import pandas as pd
import json
import pm4py


def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Flatten the OCEL for the object type 'packages'
    flattened_log = pm4py.ocel_flattening(ocel, 'packages')
    
    # Step 2: Discover the Directly-Follows Graph (DFG)
    dfg = pm4py.discover_dfg(flattened_log)
    
    # Step 3: Identify the most frequent DFG edge
    most_frequent_edge = max(dfg.items(), key=lambda x: x[1])
    
    # Step 4: Filter cases containing the most frequent DFG edge
    source_activity, target_activity = most_frequent_edge[0]
    cases_with_edge = flattened_log[(flattened_log['concept:name'].shift() == source_activity) & (flattened_log['concept:name'] == target_activity)]
    case_ids = cases_with_edge['case:concept:name'].unique()
    filtered_log = flattened_log[flattened_log['case:concept:name'].isin(case_ids)]
    
    # Step 5: Calculate average case duration
    case_durations = filtered_log.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = (case_durations['max'] - case_durations['min']).dt.total_seconds()
    average_case_duration = case_durations['duration'].mean()
    
    # Step 6: Determine the most dominant variant
    variant_counts = filtered_log['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    
    # Step 7: Save edge subset stats
    edge_subset_stats = {
        'most_frequent_edge': {'source': source_activity, 'target': target_activity, 'count': most_frequent_edge[1]},
        'average_case_duration': average_case_duration,
        'dominant_variant': dominant_variant
    }
    with open('output/edge_subset_stats.json', 'w') as f:
        json.dump(edge_subset_stats, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/edge_subset_stats.json')
    
    # Final benchmark answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'average_case_duration': average_case_duration,
        'dfg': dfg
    }
    print(json.dumps(final_answer, ensure_ascii=False))