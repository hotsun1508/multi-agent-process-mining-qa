import pm4py
import pandas as pd
import json
import os
import statistics

def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for customers
    flattened_customers = pm4py.ocel_flattening(ocel, 'customers')
    
    # Calculate case durations
    case_durations = flattened_customers.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = case_durations['max'] - case_durations['min']
    average_duration = case_durations['duration'].mean()
    
    # Isolate delayed cases
    delayed_cases = case_durations[case_durations['duration'] > average_duration].index.tolist()
    delayed_flattened = flattened_customers[flattened_customers['case:concept:name'].isin(delayed_cases)]
    
    # Discover Petri net from delayed-case subset
    petri_net = pm4py.discover_petri_net_inductive(delayed_flattened)
    png_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(petri_net, png_path)
    print(f'OUTPUT_FILE_LOCATION: {png_path}')  
    
    # Token-based replay
    fit_cases = pm4py.replay_log(petri_net, delayed_flattened)
    
    # Analyze variants
    variant_counts = pd.Series([case['variant'] for case in fit_cases]).value_counts()
    dominant_variant = variant_counts.idxmax() if not variant_counts.empty else None
    
    # Count events linked to fit cases in raw OCEL
    fit_case_ids = [case['case:concept:name'] for case in fit_cases]
    raw_events = ocel.events
    joint_count = sum(1 for event in raw_events if event['ocel:oid'] in fit_case_ids and event['ocel:type'] in ['customers', 'orders'])
    
    # Prepare final answer
    final_answer = {
        'average_case_duration': average_duration,
        'dominant_variant': dominant_variant,
        'joint_event_count': joint_count
    }
    
    # Save final answer
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))