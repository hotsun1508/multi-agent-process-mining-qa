def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the orders view
    flattened_orders = pm4py.ocel_flattening(ocel, 'orders')
    # Calculate case durations
    case_durations = flattened_orders.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
    case_durations['duration'] = case_durations['time:timestamp']['max'] - case_durations['time:timestamp']['min']
    average_duration = case_durations['duration'].mean()
    # Isolate delayed cases
    delayed_cases = case_durations[case_durations['duration'] > average_duration].index.tolist()
    # Filter raw OCEL events for delayed cases
    delayed_events = ocel.events[ocel.events['ocel:oid'].isin(delayed_cases)]
    # Count joint events linked to both orders and items
    joint_events = delayed_events[delayed_events['ocel:type'].isin(['orders', 'items'])]
    joint_event_count = joint_events['ocel:eid'].nunique()
    total_delayed_events = delayed_events['ocel:eid'].nunique()
    delayed_joint_event_ratio = joint_event_count / total_delayed_events if total_delayed_events > 0 else 0.0
    # Save the result to a JSON file
    delayed_joint_event_ratio_data = {'delayed_joint_event_ratio': delayed_joint_event_ratio}
    with open('output/delayed_joint_event_ratio.json', 'w', encoding='utf-8') as f:
        json.dump(delayed_joint_event_ratio_data, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/delayed_joint_event_ratio.json')
    # Prepare final answer
    final_answer = {'delayed_joint_event_ratio': delayed_joint_event_ratio}
    print(json.dumps(final_answer, ensure_ascii=False))