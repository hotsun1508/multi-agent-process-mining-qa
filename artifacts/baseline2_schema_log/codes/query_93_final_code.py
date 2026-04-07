import pm4py
import json
import os
import pandas as pd


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the orders view
    flat_log = pm4py.ocel_flattening(ocel, 'orders')
    
    # Discover the process model using the Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(flat_log)
    
    # Get the variants and their frequencies
    variants = flat_log['case:concept:name'].value_counts()
    top_20_percent_count = int(len(variants) * 0.2)
    top_variants = variants.nlargest(top_20_percent_count).index.tolist()
    
    # Filter the log for the top variants
    filtered_log = flat_log[flat_log['case:concept:name'].isin(top_variants)]
    
    # Perform token-based replay
    fitness = pm4py.evaluate_token_replay(filtered_log, petri_net, initial_marking)
    
    # Isolate non-fit cases
    non_fit_cases = [case for case in filtered_log['case:concept:name'].unique() if fitness[case]['fit'] == False]
    non_fit_log = filtered_log[filtered_log['case:concept:name'].isin(non_fit_cases)]
    
    # Calculate average case duration of non-fit cases
    case_durations = non_fit_log.groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds())
    average_duration = case_durations.mean()
    
    # Save the average duration to a JSON file
    nonfit_duration = {'average_case_duration': average_duration}
    os.makedirs('output', exist_ok=True)
    with open('output/nonfit_duration_tbr.json', 'w') as f:
        json.dump(nonfit_duration, f)
    print('OUTPUT_FILE_LOCATION: output/nonfit_duration_tbr.json')
    
    # Prepare the final answer
    final_answer = {'average_case_duration': average_duration, 'non_fit_cases_count': len(non_fit_cases)}
    print(json.dumps(final_answer, ensure_ascii=False))