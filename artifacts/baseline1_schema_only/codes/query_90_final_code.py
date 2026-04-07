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
    # Get events linked to delayed cases in the raw OCEL
    delayed_events = [event for event in ocel.events if event['ocel:oid'] in delayed_cases]
    # Count events linked to both orders and items
    joint_events = [event for event in delayed_events if event['ocel:type'] in ['orders', 'items']]
    joint_event_count = len(set(event['ocel:eid'] for event in joint_events))
    total_delayed_events = len(set(event['ocel:eid'] for event in delayed_events))
    delayed_joint_event_ratio = joint_event_count / total_delayed_events if total_delayed_events > 0 else 0.0
    # Save the result
    delayed_joint_event_ratio_data = {'delayed_joint_event_ratio': delayed_joint_event_ratio}
    with open('output/delayed_joint_event_ratio.json', 'w', encoding='utf-8') as f:
        json.dump(delayed_joint_event_ratio_data, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/delayed_joint_event_ratio.json')
    # Prepare final answer
    final_answer = {'delayed_joint_event_ratio': delayed_joint_event_ratio}
    print(json.dumps(final_answer, ensure_ascii=False))