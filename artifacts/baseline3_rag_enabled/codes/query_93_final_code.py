import os
import json
import pm4py
import pandas as pd


def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Flatten the OCEL for orders
    flattened_orders = pm4py.ocel_flattening(ocel, 'orders')
    
    # Step 2: Discover process variants
    variants = flattened_orders['concept:name'].value_counts().head(int(len(flattened_orders) * 0.2)).index.tolist()
    
    # Step 3: Filter the flattened log for the top variants
    filtered_log = flattened_orders[flattened_orders['concept:name'].isin(variants)]
    
    # Step 4: Discover Petri net using Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log)
    
    # Step 5: Perform token-based replay on the full flattened log
    fitness = pm4py.fitness_token_based_replay(petri_net, initial_marking, final_marking, flattened_orders)
    
    # Step 6: Isolate non-fit cases
    non_fit_cases = [case for case in fitness['non_fit_cases']]
    
    # Step 7: Calculate average duration of non-fit cases
    non_fit_durations = []
    for case in non_fit_cases:
        case_duration = flattened_orders[flattened_orders['case:concept:name'] == case]['time:timestamp'].max() - \
                        flattened_orders[flattened_orders['case:concept:name'] == case]['time:timestamp'].min()
        non_fit_durations.append(case_duration.total_seconds())
    average_non_fit_duration = sum(non_fit_durations) / len(non_fit_durations) if non_fit_durations else 0
    
    # Step 8: Save non-fit duration to JSON
    with open('output/nonfit_duration_tbr.json', 'w') as f:
        json.dump({'average_non_fit_duration': average_non_fit_duration}, f)
    print('OUTPUT_FILE_LOCATION: output/nonfit_duration_tbr.json')
    
    # Step 9: Prepare final benchmark answer
    final_answer = {
        'average_non_fit_duration': average_non_fit_duration,
        'non_fit_cases_count': len(non_fit_cases),
        'petri_net': {'places': len(petri_net.places), 'transitions': len(petri_net.transitions)}
    }
    
    # Step 10: Save final answer to CSV/log
    print(json.dumps(final_answer, ensure_ascii=False))