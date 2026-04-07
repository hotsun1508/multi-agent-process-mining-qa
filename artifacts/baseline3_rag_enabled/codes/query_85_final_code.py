import os
import pandas as pd
import json
import pm4py


def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Flatten the OCEL for the object type 'items'
    flattened_log = pm4py.ocel_flattening(ocel, 'items')
    
    # Step 2: Discover the Directly-Follows Graph (DFG)
    dfg, start_activities, end_activities = pm4py.discover_dfg(flattened_log)
    
    # Step 3: Identify the most frequent DFG edge
    total_events = sum(dfg.values())
    most_frequent_edge = max(dfg.items(), key=lambda x: x[1])
    edge_source, edge_target = most_frequent_edge[0]
    edge_count = most_frequent_edge[1]
    
    # Step 4: Filter cases containing the most frequent edge
    filtered_cases = flattened_log[flattened_log['concept:name'].isin([edge_source, edge_target])]
    case_durations = filtered_cases.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = (case_durations['max'] - case_durations['min']).dt.total_seconds()
    average_case_duration = case_durations['duration'].mean()
    
    # Step 5: Save the edge duration information
    edge_duration_info = {
        'edge': {'source': edge_source, 'target': edge_target},
        'count': edge_count,
        'average_case_duration': average_case_duration
    }
    with open('output/edge_duration_items.json', 'w', encoding='utf-8') as f:
        json.dump(edge_duration_info, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/edge_duration_items.json')
    
    # Step 6: Prepare the final answer
    final_answer = {
        'average_case_duration': average_case_duration,
        'most_frequent_edge': (edge_source, edge_target, edge_count)
    }
    print(json.dumps(final_answer, ensure_ascii=False))