import pm4py
import pandas as pd
import numpy as np
import json
import os
import statistics

def main():
    event_log = ACTIVE_LOG
    # Step 1: Convert the event log to a case dataframe
    df = pm4py.convert_to_dataframe(event_log)
    
    # Step 2: Get the top 20% most frequent variants
    variant_counts = df['case:concept:name'].value_counts()
    top_variants = variant_counts.head(int(len(variant_counts) * 0.2)).index.tolist()
    filtered_df = df[df['case:concept:name'].isin(top_variants)]
    
    # Step 3: Discover a Petri net using the Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_df)
    
    # Step 4: Perform token-based replay conformance checking
    replay_results = pm4py.conformance_diagnostics_token_based_replay(filtered_df, petri_net, initial_marking, final_marking)
    
    # Step 5: Identify fitting cases
    fitting_cases = [result for result in replay_results if result['trace_is_fit']]
    fitting_case_ids = [result['trace'] for result in fitting_cases]
    
    # Step 6: Determine the dominant variant among fitting cases
    fitting_variants = pd.Series([case['case:concept:name'] for case in fitting_cases])
    dominant_variant = fitting_variants.value_counts().idxmax()
    
    # Step 7: Calculate the median throughput time of the dominant variant
    dominant_cases = df[df['case:concept:name'] == dominant_variant]
    dominant_cases['time:timestamp'] = pd.to_datetime(dominant_cases['time:timestamp'])
    throughput_times = dominant_cases.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
    throughput_times['throughput_time'] = (throughput_times['time:timestamp']['max'] - throughput_times['time:timestamp']['min']).dt.total_seconds() / 3600
    median_throughput_time = throughput_times['throughput_time'].median()
    
    # Step 8: Save the Petri net model as a .pkl file
    petri_net_filename = 'output/petri_net_model.pkl'
    pm4py.save_vis_petri_net(petri_net, petri_net_filename)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_filename}')  
    
    # Step 9: Prepare the final benchmark answer as a JSON-serializable dictionary
    answer = {
        'primary_answer_in_csv_log': True,
        'result_type': 'composite',
        'view': 'event_log',
        'result_schema': {
            'process_discovery': 'petri_net',
            'conformance': 'conformance_summary',
            'behavior_variant': 'dominant_variant',
            'performance': 'median_throughput_time'
        },
        'artifacts_schema': ['output/* (optional auxiliary artifacts such as png/csv/pkl/json)'],
        'dominant_variant': dominant_variant,
        'median_throughput_time': median_throughput_time
    }
    
    # Step 10: Save the final result dictionary to a CSV/log file
    with open('output/result.json', 'w') as f:
        json.dump(answer, f, ensure_ascii=False)
    print(f'OUTPUT_FILE_LOCATION: output/result.json')
    
    print(json.dumps(answer, ensure_ascii=False))