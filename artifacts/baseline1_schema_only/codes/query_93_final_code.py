import pm4py
import json
import pandas as pd


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the orders view
    flat_log = pm4py.ocel_flattening(ocel, 'orders')
    
    # Discover the process model using the Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(flat_log)
    
    # Get the variants and their frequencies
    variants = flat_log['case:concept:name'].value_counts(normalize=True)
    top_20_percent_threshold = 0.2
    top_variants = variants[variants.cumsum() <= top_20_percent_threshold].index.tolist()
    
    # Filter the log for the top 20% variants
    filtered_log = flat_log[flat_log['case:concept:name'].isin(top_variants)]
    
    # Perform token-based replay
    fitness = pm4py.evaluate_token_based_replay(filtered_log, petri_net, initial_marking, final_marking)
    
    # Isolate non-fit cases
    non_fit_cases = [case for case in filtered_log['case:concept:name'].unique() if case not in fitness['fit_cases']]
    non_fit_log = filtered_log[filtered_log['case:concept:name'].isin(non_fit_cases)]
    
    # Calculate average case duration of non-fit cases
    non_fit_durations = non_fit_log.groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).reset_index()
    average_duration = non_fit_durations['time:timestamp'].mean()
    
    # Save the average duration to a JSON file
    with open('output/nonfit_duration_tbr.json', 'w') as f:
        json.dump({'average_duration': average_duration}, f)
    print('OUTPUT_FILE_LOCATION: output/nonfit_duration_tbr.json')
    
    # Prepare the final answer
    final_answer = {
        'average_non_fit_duration': average_duration,
        'total_non_fit_cases': len(non_fit_cases),
        'total_cases': len(filtered_log['case:concept:name'].unique()),
    }
    print(json.dumps(final_answer, ensure_ascii=False))