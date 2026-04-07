def main():
    ocel = ACTIVE_LOG
    # Step 1: Flatten the OCEL to get the items view
    flattened_items = pm4py.ocel_flattening(ocel, 'items')
    # Step 2: Identify the most dominant variant
    variant_counts = flattened_items['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    dominant_variant_count = variant_counts.max()
    # Step 3: Filter raw OCEL for events linked to the dominant variant
    dominant_cases = flattened_items[flattened_items['concept:name'] == dominant_variant]['case:concept:name'].unique()
    raw_events = ocel.events
    joint_events_count = len(raw_events[(raw_events['ocel:oid'].isin(dominant_cases)) & (raw_events['ocel:type'].isin(['items', 'orders']))])
    # Step 4: Calculate average case duration
    case_durations = flattened_items.groupby('case:concept:name')['time:timestamp'].max() - flattened_items.groupby('case:concept:name')['time:timestamp'].min()
    average_duration = case_durations.mean()
    # Step 5: Filter cases whose duration exceeds the average
    long_duration_cases = case_durations[case_durations > average_duration].index
    filtered_flattened_items = flattened_items[flattened_items['case:concept:name'].isin(long_duration_cases)]
    # Step 6: Discover a Petri net from the filtered cases
    petri_net = pm4py.discover_petri_net_inductive(filtered_flattened_items)
    # Step 7: Compute token-based replay fitness on the full flattened items view
    fitness = pm4py.fitness_token_based_replay(petri_net, flattened_items)
    # Step 8: Save outputs
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    with open('output/petri_net.pkl', 'wb') as f:
        pickle.dump(petri_net, f)
    print('OUTPUT_FILE_LOCATION: output/petri_net.pkl')
    final_answer = {
        'dominant_variant': dominant_variant,
        'dominant_variant_count': dominant_variant_count,
        'joint_events_count': joint_events_count,
        'average_case_duration': average_duration,
        'fitness': fitness
    }
    print(json.dumps(final_answer, ensure_ascii=False))