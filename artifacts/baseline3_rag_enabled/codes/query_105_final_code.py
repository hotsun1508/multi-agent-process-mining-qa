import os
import pandas as pd
import json
import pm4py


def main():
    ocel = ACTIVE_LOG
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL for the object type 'orders'
    flattened_log = pm4py.ocel_flattening(ocel, 'orders')

    # Step 2: Discover the DFG from the flattened log
    dfg = pm4py.discover_dfg(flattened_log)

    # Step 3: Identify the most frequent DFG edge
    most_frequent_edge = max(dfg.items(), key=lambda x: x[1])
    source_activity, target_activity = most_frequent_edge[0]
    most_frequent_edge_count = most_frequent_edge[1]

    # Step 4: Filter cases containing the most frequent DFG edge
    cases_with_edge = flattened_log[(flattened_log['concept:name'] == source_activity) | (flattened_log['concept:name'] == target_activity)]
    case_ids_with_edge = cases_with_edge['case:concept:name'].unique()

    # Step 5: Calculate average case duration for the filtered cases
    case_durations = flattened_log[flattened_log['case:concept:name'].isin(case_ids_with_edge)].groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).mean()

    # Step 6: Count events linked to orders and customers objects
    orders_objects = ocel.objects[ocel.objects['ocel:type'] == 'orders']['ocel:oid']
    customers_objects = ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid']
    relevant_events = ocel.relations[(ocel.relations['ocel:oid'].isin(orders_objects)) | (ocel.relations['ocel:oid'].isin(customers_objects))]
    joint_event_count = relevant_events[relevant_events['ocel:eid'].isin(cases_with_edge['ocel:eid'])].shape[0]

    # Step 7: Identify the most dominant variant
    variant_counts = flattened_log['concept:name'].value_counts()
    most_dominant_variant = variant_counts.idxmax()

    # Step 8: Prepare final answer
    final_answer = {
        'most_frequent_edge': {'source': source_activity, 'target': target_activity, 'count': most_frequent_edge_count},
        'average_case_duration': case_durations,
        'most_dominant_variant': most_dominant_variant,
        'joint_event_count': joint_event_count
    }

    # Save outputs
    with open(os.path.join(output_dir, 'final_answer.json'), 'w') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'final_answer.json')}')

    # Save DFG visualization
    dfg_path = os.path.join(output_dir, 'dfg_visualization.png')
    pm4py.save_vis_dfg(dfg, dfg_path)
    print(f'OUTPUT_FILE_LOCATION: {dfg_path}')  

    print(json.dumps(final_answer, ensure_ascii=False))