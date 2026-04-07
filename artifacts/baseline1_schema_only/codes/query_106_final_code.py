def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the items view
    flat_items_log = pm4py.ocel_flattening(ocel, 'items')
    # Discover variants and their frequencies
    variant_counts = flat_items_log['concept:name'].value_counts()
    # Select top 20% variants by frequency
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()
    # Filter the log for top variants
    sublog = flat_items_log[flat_items_log['concept:name'].isin(top_variants)]
    # Discover Petri net from the sublog
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(sublog)
    # Save the Petri net visualization
    petri_net_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, petri_net_path)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_path}')  
    # Run token-based replay
    fitness = pm4py.conformance_token_based_replay(sublog, petri_net, initial_marking, final_marking)
    # Calculate average case duration of non-fit cases
    non_fit_cases = fitness['non_fit_cases']
    average_duration_non_fit = sum([case['duration'] for case in non_fit_cases]) / len(non_fit_cases) if non_fit_cases else 0
    # Overall fitness
    overall_fitness = fitness['overall_fitness']
    # Prepare final answer
    final_answer = {
        'average_duration_non_fit': average_duration_non_fit,
        'overall_fitness': overall_fitness
    }
    # Save final answer to JSON
    with open('output/final_benchmark_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_benchmark_answer.json')
    print(json.dumps(final_answer, ensure_ascii=False))