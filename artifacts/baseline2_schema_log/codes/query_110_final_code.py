def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter OCEL for events linked to at least one orders and one customers object
    orders_objects = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'orders'}
    customers_objects = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'customers'}
    filtered_events = [event for event in ocel.events if any(rel['ocel:oid'] in orders_objects for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']) and any(rel['ocel:oid'] in customers_objects for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid'])]
    filtered_ocel = {
        'ocel:events': filtered_events,
        'ocel:objects': ocel.objects,
        'ocel:relations': ocel.relations
    }
    # Step 2: Flatten the restricted OCEL using orders as the case notion
    flattened_log = pm4py.ocel_flattening(filtered_ocel, object_type='orders')
    # Step 3: Identify the most dominant variant
    variant_counts = flattened_log['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    # Step 4: Discover a Petri net from the cases of that dominant variant
    dominant_cases = flattened_log[flattened_log['concept:name'] == dominant_variant]
    petri_net = pm4py.discover_petri_net_inductive(dominant_cases)
    # Step 5: Token-based replay on the raw OCEL
    fit_cases = dominant_cases['case:concept:name'].unique()
    joint_event_count = 0
    for event in ocel.events:
        if any(rel['ocel:oid'] in orders_objects for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']) and any(rel['ocel:oid'] in customers_objects for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']) and event['ocel:eid'] in fit_cases:
            joint_event_count += 1
    # Save outputs
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    with open('output/petri_net.pkl', 'wb') as f:
        pickle.dump(petri_net, f)
    print('OUTPUT_FILE_LOCATION: output/petri_net.pkl')
    final_answer = {
        'joint_event_count': joint_event_count,
        'dominant_variant': dominant_variant
    }
    print(json.dumps(final_answer, ensure_ascii=False))