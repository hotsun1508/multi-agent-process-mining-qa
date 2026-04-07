def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get a case-centric view of items
    flat_log = pm4py.ocel_flattening(ocel, 'items')
    # Discover the variants and their frequencies
    variants = flat_log['concept:name'].value_counts()
    top_20_percent_count = int(len(variants) * 0.2)
    top_variants = variants.nlargest(top_20_percent_count).index.tolist()
    # Filter the log for the top 20% variants
    sublog = flat_log[flat_log['concept:name'].isin(top_variants)]
    # Discover a Petri net from the sublog
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(sublog)
    # Save the Petri net visualization
    petri_net_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, petri_net_path)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_path}')  
    # Run token-based replay
    fitness = pm4py.evaluate_replay_fitness(petri_net, initial_marking, final_marking, sublog)
    # Calculate average case duration of non-fit cases
    non_fit_cases = [case for case in sublog['case:concept:name'].unique() if case not in fitness['fit_cases']]
    non_fit_durations = []
    for case in non_fit_cases:
        case_duration = sublog[sublog['case:concept:name'] == case]['time:timestamp'].max() - sublog[sublog['case:concept:name'] == case]['time:timestamp'].min()
        non_fit_durations.append(case_duration.total_seconds())
    average_non_fit_duration = sum(non_fit_durations) / len(non_fit_durations) if non_fit_durations else 0
    # Prepare final answer
    final_answer = {
        'average_non_fit_duration': average_non_fit_duration,
        'overall_fitness': fitness['overall_fitness'],
        'fit_cases': len(fitness['fit_cases']),
        'non_fit_cases': len(non_fit_cases)
    }
    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    print(json.dumps(final_answer, ensure_ascii=False))