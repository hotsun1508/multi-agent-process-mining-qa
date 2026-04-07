import os
import json
import pm4py
import pandas as pd


def main():
    ocel = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL for the object type 'orders'
    flattened_orders_log = pm4py.ocel_flattening(ocel, 'orders')

    # Step 2: Calculate case durations and isolate delayed cases
    case_durations = flattened_orders_log.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = case_durations['max'] - case_durations['min']
    average_duration = case_durations['duration'].mean()
    delayed_cases = case_durations[case_durations['duration'] > average_duration].index.tolist()

    # Step 3: Filter raw OCEL events for delayed cases
    delayed_events = ocel.events[ocel.events['ocel:oid'].isin(delayed_cases)]

    # Step 4: Count events linked to both 'orders' and 'items'
    joint_events = delayed_events[delayed_events['ocel:type'].isin(['orders', 'items'])]
    joint_event_count = joint_events['ocel:eid'].nunique()

    # Step 5: Save the result to a JSON file
    delayed_joint_event_ratio = {'delayed_joint_event_count': joint_event_count}
    with open(os.path.join(output_dir, 'delayed_joint_event_ratio.json'), 'w') as json_file:
        json.dump(delayed_joint_event_ratio, json_file)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'delayed_joint_event_ratio.json')})

    # Step 6: Prepare final answer for CSV/log
    final_answer = {
        'primary_answer_in_csv_log': True,
        'result_type': 'composite',
        'view': 'raw_ocel_or_flattened_view_as_specified',
        'result_schema': {'event_count': joint_event_count},
        'artifacts_schema': ['output/* (optional auxiliary artifacts such as png/csv/pkl/json)']
    }
    print(json.dumps(final_answer, ensure_ascii=False))