def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the orders view
    flattened_orders = pm4py.ocel_flattening(ocel, 'orders')
    # Discover the DFG from the flattened orders view
    dfg, start_activities, end_activities = pm4py.discover_dfg(flattened_orders)
    # Identify the most frequent DFG edge
    most_frequent_edge = max(dfg.items(), key=lambda x: x[1])
    most_frequent_edge_source, most_frequent_edge_target = most_frequent_edge[0]
    most_frequent_edge_count = most_frequent_edge[1]
    
    # Filter cases containing the most frequent edge
    cases_with_edge = flattened_orders[flattened_orders['concept:name'].isin([most_frequent_edge_source, most_frequent_edge_target])]
    
    # Get the most dominant variant
    variant_counts = cases_with_edge.groupby('case:concept:name')['concept:name'].value_counts().reset_index(name='count')
    dominant_variant = variant_counts.loc[variant_counts['count'].idxmax()]
    
    # Calculate average case duration
    case_durations = cases_with_edge.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
    case_durations['duration'] = case_durations['time:timestamp']['max'] - case_durations['time:timestamp']['min']
    average_case_duration = case_durations['duration'].mean()
    
    # Count events linked to the subset in raw OCEL
    orders_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'orders']['ocel:oid'])
    customers_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid'])
    joint_event_count = 0
    for event in ocel.events:
        if event['ocel:oid'] in orders_objects and event['ocel:oid'] in customers_objects:
            joint_event_count += 1
    
    # Save outputs
    output_data = {
        'most_frequent_edge': {
            'source': most_frequent_edge_source,
            'target': most_frequent_edge_target,
            'count': most_frequent_edge_count
        },
        'dominant_variant': dominant_variant['concept:name'],
        'average_case_duration': average_case_duration,
        'joint_event_count': joint_event_count
    }
    with open('output/results.json', 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/results.json')
    print(json.dumps(output_data, ensure_ascii=False))