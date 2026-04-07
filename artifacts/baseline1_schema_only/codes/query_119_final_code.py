def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for customers
    flattened_customers = pm4py.ocel_flattening(ocel, 'customers')
    # Calculate case durations
    flattened_customers['case_duration'] = flattened_customers.groupby('case:concept:name')['time:timestamp'].transform(lambda x: x.max() - x.min()).dt.total_seconds()
    # Calculate average case duration
    average_case_duration = flattened_customers['case_duration'].mean()
    # Get the top 20% variants by frequency
    variant_counts = flattened_customers['case:concept:name'].value_counts()
    top_20_percent_threshold = variant_counts.quantile(0.8)
    top_variants = variant_counts[variant_counts >= top_20_percent_threshold].index.tolist()
    # Filter cases in the top variants and with duration exceeding average
    filtered_cases = flattened_customers[(flattened_customers['case:concept:name'].isin(top_variants)) & (flattened_customers['case_duration'] > average_case_duration)]
    # Discover Petri net from the filtered cases
    petri_net = pm4py.discover_petri_net_inductive(filtered_cases)
    # Compute token-based replay fitness on the full flattened customers view
    initial_marking, final_marking = pm4py.get_initial_final_marking(petri_net)
    fitness = pm4py.fitness_token_based_replay(petri_net, flattened_customers, initial_marking, final_marking)
    # Count events linked to both customers and employees in the raw OCEL
    joint_event_count = len(ocel.relations[(ocel.relations['ocel:oid'].isin(filtered_cases['ocel:oid'])) & (ocel.relations['ocel:type'] == 'customers') & (ocel.relations['ocel:qualifier'] == 'employees')])
    # Save outputs
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    with open('output/petri_net.pkl', 'wb') as f:
        pickle.dump(petri_net, f)
    print('OUTPUT_FILE_LOCATION: output/petri_net.pkl')
    final_answer = {
        'behavior_variant': top_variants,
        'performance': average_case_duration,
        'process_discovery': {'petri_net': 'output/petri_net.pkl'},
        'conformance': fitness,
        'object_interaction': joint_event_count
    }
    print(json.dumps(final_answer, ensure_ascii=False))