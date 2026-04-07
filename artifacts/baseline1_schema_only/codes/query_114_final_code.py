def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the items view
    flattened_items = pm4py.ocel_flattening(ocel, 'items')
    # Discover the most dominant variant
    variant_counts = flattened_items['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    dominant_variant_cases = flattened_items[flattened_items['concept:name'] == dominant_variant]
    # Discover a Petri net from the dominant variant cases
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(dominant_variant_cases)
    # Perform token-based replay
    replay_result = pm4py.replay_log(dominant_variant_cases, petri_net, initial_marking)
    # Calculate average case duration of non-fit cases
    non_fit_cases = [case for case in replay_result if not case['fit']]
    non_fit_durations = [case['duration'] for case in non_fit_cases]
    average_non_fit_duration = sum(non_fit_durations) / len(non_fit_durations) if non_fit_durations else 0
    # Count events linked to non-fit cases that are linked to both items and customers
    non_fit_case_ids = [case['case_id'] for case in non_fit_cases]
    non_fit_events = ocel.events[ocel.events['case_id'].isin(non_fit_case_ids)]
    joint_events_count = len(non_fit_events[(non_fit_events['type'] == 'items') & (non_fit_events['type'] == 'customers')])
    # Save outputs
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    with open('output/petri_net.pkl', 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)
    print('OUTPUT_FILE_LOCATION: output/petri_net.pkl')
    final_answer = {
        'dominant_variant': dominant_variant,
        'average_non_fit_duration': average_non_fit_duration,
        'joint_events_count': joint_events_count
    }
    print(json.dumps(final_answer, ensure_ascii=False))