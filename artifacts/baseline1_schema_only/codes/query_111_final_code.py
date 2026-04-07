import pm4py
import pandas as pd
import json
import os
import statistics

def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the orders view
    flat_log = pm4py.ocel_flattening(ocel, 'orders')
    
    # Discover variants and their frequencies
    variant_counts = flat_log['case:concept:name'].value_counts()
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()
    
    # Filter the log for top 20% variants
    model_building_sublog = flat_log[flat_log['case:concept:name'].isin(top_variants)]
    
    # Discover Petri net from the model-building sublog
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(model_building_sublog)
    pm4py.save_vis_petri_net((petri_net, initial_marking, final_marking), 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    
    # Token-based replay
    replay_result = pm4py.replay_log(model_building_sublog, petri_net, initial_marking, final_marking)
    
    # Isolate non-fit cases
    non_fit_cases = [case for case in replay_result if not case['fit']]
    non_fit_count = len(non_fit_cases)
    
    # Calculate average case duration for the subset
    case_durations = model_building_sublog.groupby('case:concept:name')['time:timestamp'].apply(lambda x: (x.max() - x.min()).total_seconds()).tolist()
    average_duration = statistics.mean(case_durations) if case_durations else 0
    
    # Count non-fit cases whose duration exceeds the average duration
    delayed_non_fit_cases = [case for case in non_fit_cases if (case['case:concept:name'] in model_building_sublog['case:concept:name'].values) and ((model_building_sublog[model_building_sublog['case:concept:name'] == case['case:concept:name']]['time:timestamp'].max() - model_building_sublog[model_building_sublog['case:concept:name'] == case['case:concept:name']]['time:timestamp'].min()).total_seconds() > average_duration)]
    delayed_non_fit_count = len(delayed_non_fit_cases)
    
    # Count events linked to non-fit and delayed cases in raw OCEL
    non_fit_delayed_case_ids = [case['case:concept:name'] for case in delayed_non_fit_cases]
    events_linked = ocel.events[ocel.events['case:concept:name'].isin(non_fit_delayed_case_ids)]
    orders_objects = ocel.objects[ocel.objects['ocel:type'] == 'orders']['ocel:oid'].unique()
    items_objects = ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid'].unique()
    linked_events_count = events_linked[(events_linked['ocel:oid'].isin(orders_objects)) & (events_linked['ocel:oid'].isin(items_objects))].shape[0]
    
    # Prepare final answer
    final_answer = {
        'non_fit_count': non_fit_count,
        'delayed_non_fit_count': delayed_non_fit_count,
        'linked_events_count': linked_events_count,
        'average_case_duration': average_duration
    }
    
    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))