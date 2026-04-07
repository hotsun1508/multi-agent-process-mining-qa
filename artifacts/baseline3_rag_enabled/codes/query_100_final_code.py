import os
import json
import pm4py
import pandas as pd


def main():
    ocel = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL for the object type 'customers'
    flattened_log = pm4py.ocel_flattening(ocel, 'customers')

    # Step 2: Calculate case durations and identify the most dominant variant
    case_durations = flattened_log.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max']).reset_index()
    case_durations['duration'] = (case_durations['max'] - case_durations['min']).dt.total_seconds()
    average_duration = case_durations['duration'].mean()

    # Step 3: Identify the most dominant variant
    variant_counts = flattened_log['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()

    # Step 4: Filter cases of the dominant variant with duration exceeding average
    filtered_cases = case_durations[(case_durations['duration'] > average_duration) & (case_durations['case:concept:name'].isin(variant_counts[variant_counts.index == dominant_variant].index))]
    filtered_case_names = filtered_cases['case:concept:name']

    # Step 5: Count events linked to both customers and employees in the raw OCEL
    joint_events_count = 0
    for case_name in filtered_case_names:
        events = ocel.relations[ocel.relations['case:concept:name'] == case_name]
        customer_events = events[events['ocel:type'] == 'customers']
        employee_events = events[events['ocel:type'] == 'employees']
        joint_events_count += len(customer_events) * len(employee_events)

    # Step 6: Save the result to a JSON file
    result_json_path = os.path.join(output_dir, 'dom_variant_delayed_joint_events.json')
    with open(result_json_path, 'w') as json_file:
        json.dump({'joint_events_count': joint_events_count}, json_file)
    print(f'OUTPUT_FILE_LOCATION: {result_json_path}')  

    # Step 7: Prepare final answer for CSV/log
    final_answer = {
        'dominant_variant': dominant_variant,
        'average_case_duration': average_duration,
        'joint_events_count': joint_events_count
    }
    print(json.dumps(final_answer, ensure_ascii=False))