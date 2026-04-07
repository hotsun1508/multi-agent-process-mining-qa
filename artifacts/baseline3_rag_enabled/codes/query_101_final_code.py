import os
import pandas as pd
import json
import pm4py
import statistics

def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Flatten the OCEL for orders
    flat_log = pm4py.ocel_flattening(ocel, 'orders')
    
    # Step 2: Discover process variants
    variants = flat_log['concept:name'].value_counts()
    top_20_percent_count = int(len(variants) * 0.2)
    top_variants = variants.nlargest(top_20_percent_count).index.tolist()
    
    # Step 3: Filter the log for top variants
    sublog = flat_log[flat_log['concept:name'].isin(top_variants)]
    
    # Step 4: Discover Petri net from the sublog
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(sublog)
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    
    # Step 5: Calculate average case duration in the sublog
    case_durations = sublog.groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).tolist()
    average_duration = statistics.mean(case_durations)
    
    # Step 6: Token-based replay on the full flattened log
    fitness = pm4py.token_based_replay(petri_net, initial_marking, final_marking, flat_log)
    non_fit_cases = [case for case in fitness['non_fit_cases']]
    
    # Step 7: Calculate percentage of non-fit cases exceeding average duration
    non_fit_durations = [sublog[sublog['case:concept:name'] == case]['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()) for case in non_fit_cases]
    exceeding_cases = sum(1 for duration in non_fit_durations if duration > average_duration)
    exceeding_cases_ratio = exceeding_cases / len(non_fit_cases) if non_fit_cases else 0.0
    
    # Step 8: Save final benchmark answer
    final_answer = {
        'exceeding_cases_ratio': exceeding_cases_ratio,
        'petri_net': 'output/petri_net.png',
        'top_variants': top_variants
    }
    with open('output/benchmark_result.json', 'w') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/benchmark_result.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))