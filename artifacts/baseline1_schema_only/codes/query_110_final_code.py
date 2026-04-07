def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter OCEL for events linked to at least one orders and one customers object
    filtered_events = [event for event in ocel.events if any(rel['ocel:type'] == 'orders' for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']) and any(rel['ocel:type'] == 'customers' for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid'])]
    filtered_ocel = {'events': filtered_events, 'objects': ocel.objects, 'relations': ocel.relations}

    # Step 2: Flatten the restricted OCEL using orders as the case notion
    flattened_ocel = pm4py.ocel_flattening(filtered_ocel, 'orders')

    # Step 3: Identify the most dominant variant in the flattened view
    variant_counts = {}  
    for case in flattened_ocel:
        variant = tuple(case['concept:name'])
        if variant in variant_counts:
            variant_counts[variant] += 1
        else:
            variant_counts[variant] = 1
    dominant_variant = max(variant_counts, key=variant_counts.get)

    # Step 4: Discover a Petri net from the cases of the dominant variant
    dominant_cases = [case for case in flattened_ocel if tuple(case['concept:name']) == dominant_variant]
    petri_net = pm4py.discover_petri_net_inductive(dominant_cases)

    # Step 5: Run token-based replay on the raw OCEL
    token_based_replay_results = pm4py.replay_log(petri_net, ocel)

    # Step 6: Count events linked to fit cases that are linked to both orders and customers
    fit_cases = [case for case in token_based_replay_results if case['status'] == 'fit']
    joint_event_count = 0
    for case in fit_cases:
        for event in case['events']:
            if any(rel['ocel:type'] == 'orders' for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']) and any(rel['ocel:type'] == 'customers' for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']):
                joint_event_count += 1

    # Save outputs
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    with open('output/joint_event_count.json', 'w') as f:
        json.dump({'joint_event_count': joint_event_count}, f)
    print('OUTPUT_FILE_LOCATION: output/joint_event_count.json')

    # Final answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'joint_event_count': joint_event_count
    }
    print(json.dumps(final_answer, ensure_ascii=False))