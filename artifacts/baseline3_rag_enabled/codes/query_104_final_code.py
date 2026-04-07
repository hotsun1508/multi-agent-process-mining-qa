import os
import json
import pm4py
import pandas as pd
import statistics

def main():
    ocel = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL for the object type 'items'
    flattened_items_log = pm4py.ocel_flattening(ocel, 'items')

    # Step 2: Calculate case durations and isolate delayed cases
    case_durations = flattened_items_log.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = case_durations['max'] - case_durations['min']
    average_duration = statistics.mean(case_durations['duration'])
    delayed_cases = case_durations[case_durations['duration'] > average_duration].index.tolist()

    # Step 3: Map delayed cases back to raw OCEL events
    delayed_events = ocel.events[ocel.events['case:concept:name'].isin(delayed_cases)]

    # Step 4: Filter events linked to at least one items object and one customers object
    items_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid'])
    customers_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid'])
    filtered_events = delayed_events[delayed_events['ocel:oid'].isin(items_objects) & delayed_events['ocel:oid'].isin(customers_objects)]

    # Step 5: Create a restricted OCEL
    restricted_ocel = pm4py.ocel_from_events(filtered_events)

    # Step 6: Flatten the restricted OCEL using items as the case notion
    flattened_restricted_log = pm4py.ocel_flattening(restricted_ocel, 'items')

    # Step 7: Discover the most dominant variant
    variant_counts = flattened_restricted_log['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()

    # Step 8: Discover DFG on the cases of the dominant variant
    dfg, start_activities, end_activities = pm4py.discover_dfg(flattened_restricted_log[flattened_restricted_log['concept:name'] == dominant_variant])

    # Step 9: Save DFG visualization
    dfg_path = os.path.join(output_dir, 'dfg_visualization.png')
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_path)
    print(f'OUTPUT_FILE_LOCATION: {dfg_path}')  

    # Step 10: Prepare final answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'average_case_duration': average_duration,
    }

    # Step 11: Save final answer to JSON
    with open(os.path.join(output_dir, 'final_answer.json'), 'w') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'final_answer.json')}')

    print(json.dumps(final_answer, ensure_ascii=False))