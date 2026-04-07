import os
import pandas as pd
import pm4py
import json


def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Flatten the OCEL to get the orders view
    flattened_orders = pm4py.ocel_flattening(ocel, 'orders')
    
    # Step 2: Discover process variants and their frequencies
    variants = flattened_orders['concept:name'].value_counts()
    top_20_percent_count = int(len(variants) * 0.2)
    top_variants = variants.nlargest(top_20_percent_count).index.tolist()
    
    # Step 3: Filter the flattened log for the top variants
    filtered_log = flattened_orders[flattened_orders['concept:name'].isin(top_variants)]
    
    # Step 4: Discover a Petri net using the Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log)
    
    # Step 5: Perform token-based replay on the full flattened orders view
    fitness = pm4py.fitness_token_based_replay(flattened_orders, petri_net, initial_marking, final_marking)
    
    # Step 6: Isolate non-fit cases
    non_fit_cases = [case for case in fitness if fitness[case]['fit'] == False]
    
    # Step 7: Calculate the most dominant variant among non-fit cases
    non_fit_variants = flattened_orders[flattened_orders['case:concept:name'].isin(non_fit_cases)]['concept:name'].value_counts()
    dominant_non_fit_variant = non_fit_variants.idxmax()
    average_case_duration = flattened_orders[flattened_orders['case:concept:name'].isin(non_fit_cases)].groupby('case:concept:name')['time:timestamp'].apply(lambda x: (x.max() - x.min()).total_seconds()).mean()
    
    # Step 8: Count events linked to non-fit cases that are linked to both orders and items in the raw OCEL
    non_fit_orders = flattened_orders[flattened_orders['case:concept:name'].isin(non_fit_cases)]['case:concept:name'].unique()
    raw_ocel_events = ocel.events
    non_fit_event_count = len(raw_ocel_events[(raw_ocel_events['ocel:oid'].isin(non_fit_orders)) & (raw_ocel_events['ocel:type'] == 'items')])
    
    # Step 9: Save outputs
    pm4py.save_vis_petri_net((petri_net, initial_marking, final_marking), os.path.join(output_dir, 'petri_net.png'))
    with open(os.path.join(output_dir, 'non_fit_cases.json'), 'w') as f:
        json.dump(non_fit_cases, f)
    
    # Step 10: Prepare final answer
    final_answer = {
        'dominant_non_fit_variant': dominant_non_fit_variant,
        'average_case_duration': average_case_duration,
        'non_fit_event_count': non_fit_event_count
    }
    
    # Step 11: Save final answer to CSV/log
    with open(os.path.join(output_dir, 'final_benchmark_answer.json'), 'w') as f:
        json.dump(final_answer, f)
    
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'final_benchmark_answer.json')}')
    print(json.dumps(final_answer, ensure_ascii=False))