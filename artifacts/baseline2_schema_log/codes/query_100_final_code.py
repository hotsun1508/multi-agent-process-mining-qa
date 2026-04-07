def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for customers
    flattened_customers = pm4py.ocel_flattening(ocel, 'customers')
    # Calculate case durations
    case_durations = flattened_customers.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = case_durations['max'] - case_durations['min']
    average_duration = case_durations['duration'].mean()
    # Identify the most dominant variant
    variant_counts = flattened_customers['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    # Filter cases of the dominant variant with duration exceeding average
    filtered_cases = case_durations[case_durations['duration'] > average_duration].index.tolist()
    dominant_variant_cases = flattened_customers[flattened_customers['case:concept:name'].isin(filtered_cases) & (flattened_customers['concept:name'] == dominant_variant)]
    # Get the raw OCEL events linked to the filtered cases
    raw_events = ocel.events
    filtered_event_ids = dominant_variant_cases['case:concept:name'].unique()
    joint_events = []
    for event in raw_events:
        if event['ocel:oid'] in filtered_event_ids:
            # Check if the event is linked to both customers and employees
            customer_linked = any(rel['ocel:type'] == 'customers' for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid'])
            employee_linked = any(rel['ocel:type'] == 'employees' for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid'])
            if customer_linked and employee_linked:
                joint_events.append(event)
    # Save the joint events to a JSON file
    with open('output/dom_variant_delayed_joint_events.json', 'w', encoding='utf-8') as f:
        json.dump(joint_events, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/dom_variant_delayed_joint_events.json')
    # Prepare the final answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'average_case_duration': average_duration,
        'filtered_cases_count': len(filtered_cases),
        'joint_events_count': len(joint_events)
    }
    print(json.dumps(final_answer, ensure_ascii=False))