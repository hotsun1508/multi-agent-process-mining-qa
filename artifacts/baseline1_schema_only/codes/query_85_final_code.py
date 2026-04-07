import pm4py
import json
import pandas as pd


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for items
    flat_items = pm4py.ocel_flattening(ocel, 'items')
    
    # Discover DFG
    dfg, start_activities, end_activities = pm4py.discover_dfg(flat_items)
    
    # Find the most frequent DFG edge
    total_events = sum(dfg.values())
    most_frequent_edge = max(dfg.items(), key=lambda x: x[1])
    source, target = most_frequent_edge[0]
    edge_count = most_frequent_edge[1]
    
    # Filter cases containing the most frequent edge
    cases_with_edge = flat_items[(flat_items['concept:name'] == source) | (flat_items['concept:name'] == target)]
    
    # Calculate average case duration
    case_durations = cases_with_edge.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = (case_durations['max'] - case_durations['min']).dt.total_seconds()
    average_duration = case_durations['duration'].mean()
    
    # Save edge duration results
    edge_duration_result = {
        'source': source,
        'target': target,
        'count': edge_count,
        'average_case_duration': average_duration
    }
    with open('output/edge_duration_items.json', 'w', encoding='utf-8') as f:
        json.dump(edge_duration_result, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/edge_duration_items.json')
    
    # Prepare final answer
    final_answer = {
        'most_frequent_edge': {'source': source, 'target': target, 'count': edge_count},
        'average_case_duration': average_duration
    }
    print(json.dumps(final_answer, ensure_ascii=False))