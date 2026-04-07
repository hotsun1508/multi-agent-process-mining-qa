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

    # Step 2: Calculate case durations and average case duration
    case_durations = flattened_customers.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
    case_durations['duration'] = case_durations['time:timestamp']['max'] - case_durations['time:timestamp']['min']
    average_duration = case_durations['duration'].mean()

    # Step 3: Isolate delayed cases
    delayed_cases = case_durations[case_durations['duration'] > average_duration].index.tolist()
    delayed_flattened = flattened_customers[flattened_customers['case:concept:name'].isin(delayed_cases)]

    # Step 4: Discover Petri net from delayed-case subset
    petri_net = pm4py.discover_petri_net_inductive(delayed_flattened)
    pm4py.save_vis_petri_net(petri_net, os.path.join(output_dir, 'petri_net.png'))
    with open(os.path.join(output_dir, 'petri_net.pkl'), 'wb') as f:
        pickle.dump(petri_net, f)

    # Step 5: Token-based replay on the delayed-case subset
    fit_cases = pm4py.replay_fitness(petri_net, delayed_flattened)
    dominant_variant = fit_cases['variants'].most_common(1)[0][0] if fit_cases['variants'] else None

    # Step 6: Count events linked to fit cases in raw OCEL
    fit_case_events = delayed_flattened['case:concept:name'].unique()
    joint_events_count = ocel.relations[(ocel.relations['oid'].isin(fit_case_events)) & (ocel.relations['type'] == 'orders')].shape[0]

    # Step 7: Prepare final answer
    final_answer = {
        'average_case_duration': average_duration,
        'delayed_cases_count': len(delayed_cases),
        'dominant_variant': dominant_variant,
        'joint_events_count': joint_events_count
    }

    # Save final answer
    with open(os.path.join(output_dir, 'final_answer.json'), 'w') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)

    print(json.dumps(final_answer, ensure_ascii=False))