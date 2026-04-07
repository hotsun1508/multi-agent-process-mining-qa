import pm4py
import pandas as pd
import numpy as np
import json
import os
import statistics
from pm4py.objects.ocel import util as ocel_util


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the orders view
    flattened_orders = pm4py.ocel_flattening(ocel, 'orders')
    
    # Discover variants and their frequencies
    variants = flattened_orders['concept:name'].value_counts()
    top_20_percent_count = int(len(variants) * 0.2)
    top_variants = variants.nlargest(top_20_percent_count).index.tolist()
    
    # Create a sublog with the top 20% variants
    sublog = flattened_orders[flattened_orders['concept:name'].isin(top_variants)]
    
    # Discover a Petri net from the sublog
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(sublog)
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    
    # Token-based replay
    fit_cases = pm4py.conformance_token_based_replay(sublog, petri_net, initial_marking)
    non_fit_cases = [case for case in fit_cases if not case['fit']]
    
    # Calculate case durations and average duration
    case_durations = sublog.groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).reset_index()
    average_duration = case_durations['time:timestamp'].mean()
    
    # Isolate non-fit cases whose duration exceeds the average
    non_fit_case_names = [case['case:concept:name'] for case in non_fit_cases]
    delayed_cases = case_durations[(case_durations['case:concept:name'].isin(non_fit_case_names)) & (case_durations['time:timestamp'] > average_duration)]
    
    # Count events linked to non-fit and delayed cases in raw OCEL
    non_fit_delayed_case_ids = delayed_cases['case:concept:name'].tolist()
    linked_events = ocel_util.get_events(ocel)
    count_linked_events = sum(1 for event in linked_events if event['case:concept:name'] in non_fit_delayed_case_ids and event['ocel:oid'] in ocel.objects['orders'] and event['ocel:oid'] in ocel.objects['items'])
    
    # Prepare final answer
    final_answer = {
        'non_fit_cases_count': len(non_fit_cases),
        'delayed_cases_count': len(delayed_cases),
        'count_linked_events': count_linked_events,
        'average_case_duration': average_duration
    }
    
    # Save final answer
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))