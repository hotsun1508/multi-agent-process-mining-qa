import os
import pandas as pd
import pm4py
import json


def main():
    ocel = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL to get the orders view
    flattened_orders = pm4py.ocel_flattening(ocel, 'orders')

    # Step 2: Calculate the frequency of variants
    variant_counts = flattened_orders['concept:name'].value_counts()
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()

    # Step 3: Filter the flattened log for top variants
    filtered_log = flattened_orders[flattened_orders['concept:name'].isin(top_variants)]

    # Step 4: Calculate case durations and average duration
    case_durations = filtered_log.groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).reset_index()
    average_duration = case_durations['time:timestamp'].mean()

    # Step 5: Discover Petri net using Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log)
    pm4py.save_vis_petri_net(petri_net, os.path.join(output_dir, 'petri_net.png'))
    with open(os.path.join(output_dir, 'petri_net.pkl'), 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)

    # Step 6: Token-based replay to check fitness
    fitness_results = pm4py.replay_fitness(filtered_log, petri_net, initial_marking, final_marking)
    non_fit_cases = [case for case, fit in fitness_results.items() if not fit]

    # Step 7: Calculate percentage of non-fit cases exceeding average duration
    non_fit_case_durations = case_durations[case_durations['case:concept:name'].isin(non_fit_cases)]
    exceeding_cases = non_fit_case_durations[non_fit_case_durations['time:timestamp'] > average_duration]
    exceeding_cases_ratio = len(exceeding_cases) / len(non_fit_case_durations) if len(non_fit_case_durations) > 0 else 0.0

    # Step 8: Count events linked to non-fit cases in raw OCEL
    non_fit_events = ocel.events[ocel.events['case:concept:name'].isin(non_fit_cases)]
    joint_events_count = non_fit_events[(non_fit_events['ocel:type'] == 'orders') & (non_fit_events['ocel:qualifier'] == 'customers')].shape[0]

    # Step 9: Save outputs
    final_answer = {
        'exceeding_cases_ratio': exceeding_cases_ratio,
        'non_fit_cases_count': len(non_fit_cases),
        'joint_events_count': joint_events_count
    }
    with open(os.path.join(output_dir, 'final_answer.json'), 'w') as f:
        json.dump(final_answer, f)

    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'final_answer.json')}')
    print(json.dumps(final_answer, ensure_ascii=False))