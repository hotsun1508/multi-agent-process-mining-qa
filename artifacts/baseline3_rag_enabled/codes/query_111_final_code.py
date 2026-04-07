import os
import pandas as pd
import json
import pm4py


def main():
    ocel = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL to get the orders view
    flattened_orders = pm4py.ocel_flattening(ocel, 'orders')

    # Step 2: Get the frequency of variants
    variant_counts = flattened_orders['case:concept:name'].value_counts()
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()

    # Step 3: Filter the flattened log for top variants
    sublog = flattened_orders[flattened_orders['case:concept:name'].isin(top_variants)]

    # Step 4: Discover a Petri net from the sublog
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(sublog)
    pm4py.save_vis_petri_net(petri_net, os.path.join(output_dir, 'petri_net.png'))
    with open(os.path.join(output_dir, 'petri_net.pkl'), 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)

    # Step 5: Token-based replay on the sublog
    replay_results = pm4py.algo.conformance.token_based_replay.apply(sublog, petri_net, initial_marking)
    non_fit_cases = [case for case, fit in zip(replay_results['case_ids'], replay_results['fit']) if not fit]

    # Step 6: Calculate average case duration for the sublog
    case_durations = sublog.groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).reset_index()
    average_duration = case_durations['time:timestamp'].mean()

    # Step 7: Identify non-fit cases exceeding average duration
    non_fit_durations = case_durations[case_durations['case:concept:name'].isin(non_fit_cases)]
    exceeding_cases = non_fit_durations[non_fit_durations['time:timestamp'] > average_duration]
    exceeding_cases_ratio = len(exceeding_cases) / len(non_fit_cases) if non_fit_cases else 0.0

    # Step 8: Count events linked to non-fit and delayed cases in raw OCEL
    delayed_cases = non_fit_durations[non_fit_durations['time:timestamp'] > average_duration]['case:concept:name'].tolist()
    delayed_events = ocel.events[ocel.events['case:concept:name'].isin(delayed_cases)]
    linked_events_count = delayed_events[(delayed_events['ocel:oid'].isin(ocel.objects[ocel.objects['ocel:type'] == 'orders']['ocel:oid'])) & (delayed_events['ocel:oid'].isin(ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid']))].shape[0]

    # Step 9: Save final benchmark answer
    final_answer = {
        'exceeding_cases_ratio': exceeding_cases_ratio,
        'linked_events_count': linked_events_count
    }
    with open(os.path.join(output_dir, 'benchmark_answer.json'), 'w') as f:
        json.dump(final_answer, f)

    print(json.dumps(final_answer, ensure_ascii=False))