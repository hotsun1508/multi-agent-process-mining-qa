import pm4py
import json
import os
import pandas as pd


def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter the OCEL to events linked to at least one orders and one customers object
    orders_objects = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'orders'}
    customers_objects = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'customers'}
    filtered_events = [event for event in ocel.events if event['ocel:oid'] in orders_objects and event['ocel:oid'] in customers_objects]
    filtered_ocel = pm4py.objects.ocel.obj.OCEL(events=filtered_events, objects=ocel.objects, relations=ocel.relations)

    # Step 2: Flatten the restricted OCEL using orders as the case notion
    flattened_ocel = pm4py.ocel_flattening(filtered_ocel, object_type='orders')

    # Step 3: Find the most dominant variant
    log_df = pm4py.convert_to_dataframe(flattened_ocel)
    dominant_variant = log_df['concept:name'].value_counts().idxmax()

    # Step 4: Discover a DFG on the cases of that dominant variant
    variant_cases = log_df[log_df['concept:name'] == dominant_variant]
    dfg, start_activities, end_activities = pm4py.discover_dfg(variant_cases)
    dfg_path = 'output/dfg_visualization.png'
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_path)
    print(f'OUTPUT_FILE_LOCATION: {dfg_path}')  

    # Step 5: Calculate average case duration
    case_durations = variant_cases.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = case_durations['max'] - case_durations['min']
    average_duration = case_durations['duration'].mean()

    # Prepare final answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'average_case_duration': average_duration,
    }
    with open('output/final_benchmark_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_benchmark_answer.json')

    print(json.dumps(final_answer, ensure_ascii=False))