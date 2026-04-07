import pm4py
import pandas as pd
import json
import os
from collections import Counter


def main():
    ocel = ACTIVE_LOG
    # Step 1: Flatten the OCEL to get the orders view
    flattened_orders = pm4py.ocel_flattening(ocel, 'orders')
    log_df = pm4py.convert_to_dataframe(flattened_orders).sort_values(['case:concept:name', 'time:timestamp'])
    
    # Step 2: Discover the Petri net using the Inductive Miner on the top 20% most frequent variants
    variant_counts = log_df['case:concept:name'].value_counts()
    top_20_percent_variants = variant_counts.head(int(len(variant_counts) * 0.2)).index.tolist()
    filtered_log = log_df[log_df['case:concept:name'].isin(top_20_percent_variants)]
    petri_net = pm4py.discover_petri_net_inductive(filtered_log)
    
    # Step 3: Perform token-based replay to identify non-fit cases
    token_replay_results = pm4py.replay_log(filtered_log, petri_net)
    non_fit_cases = [case for case in token_replay_results if not case['fit']]
    
    # Step 4: Analyze non-fit cases to find the most dominant variant and calculate average case duration
    non_fit_case_names = [case['case:concept:name'] for case in non_fit_cases]
    non_fit_case_durations = log_df[log_df['case:concept:name'].isin(non_fit_case_names)].groupby('case:concept:name')['time:timestamp'].apply(lambda x: (x.max() - x.min()).total_seconds()).reset_index()
    dominant_variant = Counter(non_fit_case_names).most_common(1)[0]
    average_duration = non_fit_case_durations['time:timestamp'].mean()
    
    # Step 5: Count events linked to non-fit cases that are linked to both orders and items in the raw OCEL
    non_fit_case_ids = set(non_fit_case_names)
    raw_events = ocel.events
    count_linked_events = sum(1 for event in raw_events if event['ocel:oid'] in non_fit_case_ids and event['ocel:type'] in ['orders', 'items'])
    
    # Step 6: Save all outputs
    output_dir = 'output/'
    os.makedirs(output_dir, exist_ok=True)
    pm4py.save_vis_petri_net(petri_net, os.path.join(output_dir, 'petri_net.png'))
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'petri_net.png')}')
    
    # Final benchmark answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'average_case_duration': average_duration,
        'count_linked_events': count_linked_events
    }
    with open(os.path.join(output_dir, 'benchmark_result.json'), 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'benchmark_result.json')}')
    
    print(json.dumps(final_answer, ensure_ascii=False))