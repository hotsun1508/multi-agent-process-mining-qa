def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the orders view
    flat_orders = pm4py.ocel_flattening(ocel, 'orders')
    # Discover the DFG from the flattened orders view
    dfg, start_activities, end_activities = pm4py.discover_dfg(flat_orders)
    # Identify the most frequent DFG edge
    most_frequent_edge = max(dfg.items(), key=lambda x: x[1])
    most_frequent_edge_source, most_frequent_edge_target = most_frequent_edge[0]
    most_frequent_edge_count = most_frequent_edge[1]
    # Filter cases containing the most frequent edge
    cases_with_edge = flat_orders[(flat_orders['concept:name'] == most_frequent_edge_source) | (flat_orders['concept:name'] == most_frequent_edge_target)]
    # Get the most dominant variant
    variant_counts = cases_with_edge.groupby('case:concept:name').size().reset_index(name='counts')
    dominant_variant = variant_counts.loc[variant_counts['counts'].idxmax()]
    # Calculate average case duration
    case_durations = cases_with_edge.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = case_durations['max'] - case_durations['min']
    average_duration = case_durations['duration'].mean()
    # Count events linked to orders and customers objects
    orders_objects = set(ocel.objects['orders']['oid'])
    customers_objects = set(ocel.objects['customers']['oid'])
    joint_event_count = 0
    for case in cases_with_edge['case:concept:name'].unique():
        case_events = cases_with_edge[cases_with_edge['case:concept:name'] == case]
        linked_orders = set(case_events[case_events['ocel:type'] == 'orders']['ocel:oid'])
        linked_customers = set(case_events[case_events['ocel:type'] == 'customers']['ocel:oid'])
        if linked_orders and linked_customers:
            joint_event_count += len(case_events)
    # Save outputs
    output_data = {
        'most_frequent_edge': {
            'source': most_frequent_edge_source,
            'target': most_frequent_edge_target,
            'count': most_frequent_edge_count
        },
        'dominant_variant': dominant_variant['case:concept:name'],
        'average_case_duration': average_duration,
        'joint_event_count': joint_event_count
    }
    with open('output/results.json', 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/results.json')
    print(json.dumps(output_data, ensure_ascii=False))