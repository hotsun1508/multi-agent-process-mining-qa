import os
import json
import pandas as pd
import pm4py
from pm4py.algo.discovery import petri_net as pn_discovery
from pm4py.algo.conformance import token_replay


def main():
    event_log = ACTIVE_LOG
    # Convert the event log to a DataFrame
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df['time:timestamp'] = pd.to_datetime(log_df['time:timestamp'], utc=True, errors='coerce')
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])
    
    # Calculate case durations
    case_durations = log_df.groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).reset_index()
    case_durations.columns = ['case:concept:name', 'duration']
    
    # Determine the threshold for the slowest 10% of cases
    threshold_index = int(len(case_durations) * 0.1)
    slowest_cases = case_durations.nlargest(threshold_index, 'duration')
    slowest_case_ids = slowest_cases['case:concept:name'].tolist()
    slowest_cases_df = log_df[log_df['case:concept:name'].isin(slowest_case_ids)]
    
    # Identify the dominant variant among the slowest cases
    variants = pm4py.get_variants(slowest_cases_df)
    dominant_variant = max(variants.items(), key=lambda x: x[1])[0]
    
    # Discover a reference Petri net from the cases of that variant
    variant_cases_df = slowest_cases_df[slowest_cases_df['case:concept:name'].isin(dominant_variant)]
    petri_net, initial_marking, final_marking = pn_discovery.apply(variant_cases_df)
    pn_discovery.save_vis_petri_net(petri_net, 'output/reference_petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/reference_petri_net.png')
    
    # Identify cases that are not fit under token-based replay
    non_fit_cases = []
    for case_id in variant_cases_df['case:concept:name'].unique():
        case_log = variant_cases_df[variant_cases_df['case:concept:name'] == case_id]
        fitness = token_replay.apply(case_log, petri_net, initial_marking, final_marking)
        if fitness['fit'] == 0:
            non_fit_cases.append(case_id)
    
    # Report the top 3 resources appearing in those non-fit cases
    non_fit_cases_df = variant_cases_df[variant_cases_df['case:concept:name'].isin(non_fit_cases)]
    top_resources = non_fit_cases_df['org:resource'].value_counts().head(3).to_dict()
    
    # Prepare the final result dictionary
    final_answer = {
        'slowest_case_ids': slowest_case_ids,
        'dominant_variant': dominant_variant,
        'non_fit_cases': non_fit_cases,
        'top_resources': top_resources
    }
    
    # Save the final answer to a JSON file
    with open('output/final_benchmark_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_benchmark_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))