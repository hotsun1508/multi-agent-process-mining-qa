import os
import pandas as pd
import json
import pm4py

def main():
    ocel = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Filter events linked to 'orders' and 'customers'
    orders_objects = ocel.objects[ocel.objects['ocel:type'] == 'orders']['ocel:oid']
    customers_objects = ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid']

    # Get relations linked to orders and customers
    filtered_relations = ocel.relations[(ocel.relations['ocel:oid'].isin(orders_objects)) | (ocel.relations['ocel:oid'].isin(customers_objects))]
    filtered_events = ocel.events[ocel.events['ocel:eid'].isin(filtered_relations['ocel:eid'])]

    # Create a restricted OCEL
    restricted_ocel = pm4py.objects.ocel.obj.OCEL(events=filtered_events, objects=ocel.objects, relations=filtered_relations)

    # Step 2: Flatten the restricted OCEL using 'orders' as the case notion
    flattened_log = pm4py.ocel_flattening(restricted_ocel, 'orders')

    # Step 3: Identify the most dominant variant
    variant_counts = flattened_log['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    dominant_variant_count = variant_counts.max()

    # Step 4: Calculate average case duration for the dominant variant
    dominant_cases = flattened_log[flattened_log['concept:name'] == dominant_variant]
    average_case_duration = (dominant_cases['time:timestamp'].max() - dominant_cases['time:timestamp'].min()).total_seconds()

    # Step 5: Discover DFG on the cases of the dominant variant
    dfg, start_activities, end_activities = pm4py.discover_dfg(dominant_cases)

    # Save DFG visualization
    dfg_path = os.path.join(output_dir, 'dfg_visualization.png')
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_path)
    print(f'OUTPUT_FILE_LOCATION: {dfg_path}')  

    # Prepare final answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'average_case_duration': average_case_duration,
        'dfg': dfg
    }

    # Save final answer to JSON
    answer_path = os.path.join(output_dir, 'final_answer.json')
    with open(answer_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {answer_path}')  

    print(json.dumps(final_answer, ensure_ascii=False))