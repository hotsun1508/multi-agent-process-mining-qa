import os
import pandas as pd
import json
import pm4py
from pm4py.objects.ocel import flattening
from pm4py.algo.discovery import petri_net as pn_discovery
from pm4py.algo.conformance import token_based_replay


def main():
    ocel = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL to get the items view
    flat_items = flattening(ocel, 'items')

    # Step 2: Get the frequency of each variant
    variant_counts = flat_items['case:concept:name'].value_counts()
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()

    # Step 3: Filter the flattened log for top variants
    sublog = flat_items[flat_items['case:concept:name'].isin(top_variants)]

    # Step 4: Discover a Petri net from the sublog
    petri_net, initial_marking, final_marking = pn_discovery.apply(sublog)
    pn_path = os.path.join(output_dir, 'petri_net.png')
    pm4py.save_vis_petri_net(petri_net, pn_path)
    print(f'OUTPUT_FILE_LOCATION: {pn_path}')  

    # Step 5: Run token-based replay on the sublog
    fitness = token_based_replay.apply(sublog, petri_net, initial_marking, final_marking)

    # Step 6: Calculate average case duration of non-fit cases
    non_fit_cases = fitness['non_fit_cases']
    non_fit_durations = [case['duration'] for case in non_fit_cases]
    average_non_fit_duration = sum(non_fit_durations) / len(non_fit_durations) if non_fit_durations else 0

    # Step 7: Overall fitness
    overall_fitness = fitness['overall_fitness']

    # Step 8: Prepare final answer
    final_answer = {
        'average_non_fit_duration': average_non_fit_duration,
        'overall_fitness': overall_fitness,
        'top_variants': top_variants
    }

    # Step 9: Save final answer to JSON
    with open(os.path.join(output_dir, 'final_benchmark_answer.json'), 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'final_benchmark_answer.json')}')

    print(json.dumps(final_answer, ensure_ascii=False))