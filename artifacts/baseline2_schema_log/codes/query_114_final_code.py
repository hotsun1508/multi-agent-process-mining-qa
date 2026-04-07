def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the items view
    flat_items = pm4py.ocel_flattening(ocel, 'items')
    # Identify the most dominant variant
    variant_counts = flat_items['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    # Filter the flattened log for the dominant variant
    dominant_cases = flat_items[flat_items['concept:name'] == dominant_variant]
    # Discover a Petri net from the cases of the dominant variant
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(dominant_cases)
    # Save the Petri net visualization
    petri_net_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(petri_net, petri_net_path)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_path}')  
    # Perform token-based replay
    replay_results = pm4py.replay_log(dominant_cases, petri_net, initial_marking)
    # Calculate average case duration for non-fit cases
    non_fit_cases = [case for case in replay_results if not case['fit']]
    non_fit_durations = [case['duration'] for case in non_fit_cases]
    average_non_fit_duration = sum(non_fit_durations) / len(non_fit_durations) if non_fit_durations else 0
    # Count events linked to non-fit cases that are linked to both items and customers
    non_fit_case_ids = [case['case_id'] for case in non_fit_cases]
    raw_events = ocel.events
    joint_event_count = 0
    for event in raw_events:
        if event['case_id'] in non_fit_case_ids:
            linked_items = [relation['oid'] for relation in ocel.relations if relation['eid'] == event['eid'] and relation['type'] == 'items']
            linked_customers = [relation['oid'] for relation in ocel.relations if relation['eid'] == event['eid'] and relation['type'] == 'customers']
            if linked_items and linked_customers:
                joint_event_count += 1
    # Prepare final answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'average_non_fit_duration': average_non_fit_duration,
        'joint_event_count': joint_event_count
    }
    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    print(json.dumps(final_answer, ensure_ascii=False))