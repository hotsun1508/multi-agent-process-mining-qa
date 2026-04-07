def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for customers
    flattened_customers = pm4py.ocel_flattening(ocel, 'customers')
    # Calculate case durations
    flattened_customers['case_duration'] = flattened_customers.groupby('case:concept:name')['time:timestamp'].transform(lambda x: x.max() - x.min()).dt.total_seconds()
    # Calculate average case duration
    average_duration = flattened_customers['case_duration'].mean()
    # Identify the most dominant variant
    variant_counts = flattened_customers['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    # Filter cases of the dominant variant with duration exceeding average
    filtered_cases = flattened_customers[flattened_customers['case:concept:name'] == dominant_variant]
    delayed_cases = filtered_cases[filtered_cases['case_duration'] > average_duration]
    # Get the case IDs of the delayed cases
    delayed_case_ids = delayed_cases['case:concept:name'].unique()
    # Count joint events in the raw OCEL
    joint_events_count = 0
    for case_id in delayed_case_ids:
        # Get events linked to the case
        case_events = ocel.relations[ocel.relations['ocel:oid'] == case_id]
        # Check for events linked to both customers and employees
        customer_events = case_events[case_events['ocel:type'] == 'customers']
        employee_events = case_events[case_events['ocel:type'] == 'employees']
        joint_events_count += len(customer_events) * len(employee_events)
    # Save the joint events count to a JSON file
    with open('output/dom_variant_delayed_joint_events.json', 'w') as f:
        json.dump({'joint_events_count': joint_events_count}, f)
    print('OUTPUT_FILE_LOCATION: output/dom_variant_delayed_joint_events.json')
    # Prepare final answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'average_case_duration': average_duration,
        'delayed_cases_count': len(delayed_cases),
        'joint_events_count': joint_events_count
    }
    print(json.dumps(final_answer, ensure_ascii=False))