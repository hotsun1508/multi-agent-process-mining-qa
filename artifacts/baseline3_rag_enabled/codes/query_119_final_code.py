import os
import pandas as pd
import pm4py
import json
import statistics


def main():
    ocel = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL for customers
    flattened_customers = pm4py.ocel_flattening(ocel, 'customers')

    # Step 2: Calculate case durations and average duration
    case_durations = flattened_customers.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = (case_durations['max'] - case_durations['min']).dt.total_seconds()
    average_duration = case_durations['duration'].mean()

    # Step 3: Get the top 20% variants by frequency
    variant_counts = flattened_customers['concept:name'].value_counts()
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()

    # Step 4: Filter cases in the top variant subset with duration exceeding average
    filtered_cases = case_durations[case_durations.index.isin(top_variants) & (case_durations['duration'] > average_duration)].index.tolist()
    delayed_top_variant_subset = flattened_customers[flattened_customers['case:concept:name'].isin(filtered_cases)]

    # Step 5: Discover a Petri net from the delayed top-variant subset
    petri_net = pm4py.discover_petri_net_inductive(delayed_top_variant_subset)
    pm4py.save_vis_petri_net(petri_net, os.path.join(output_dir, 'petri_net.png'))
    with open(os.path.join(output_dir, 'petri_net.pkl'), 'wb') as f:
        pickle.dump(petri_net, f)

    # Step 6: Compute token-based replay fitness on the full flattened customers view
    fitness = pm4py.fitness_token_based_replay(petri_net, flattened_customers)

    # Step 7: Count events linked to both customers and employees in the raw OCEL
    raw_events = ocel.events
    joint_count = sum(1 for event in raw_events if event['ocel:oid'] in filtered_cases and event['ocel:type'] in ['customers', 'employees'])

    # Step 8: Prepare final answer
    final_answer = {
        'exceeding_cases_ratio': len(filtered_cases) / len(case_durations) if len(case_durations) > 0 else 0,
        'petri_net': 'petri_net.pkl',
        'top_variants': top_variants,
        'joint_event_count': joint_count
    }

    # Save final answer to JSON
    with open(os.path.join(output_dir, 'final_answer.json'), 'w') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)

    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'final_answer.json')}')
    print(json.dumps(final_answer, ensure_ascii=False))