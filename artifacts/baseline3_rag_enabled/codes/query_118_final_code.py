import os
import pandas as pd
import pm4py
import json


def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Filter events linked to at least one customer and one employee
    customer_ids = set(ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid'])
    employee_ids = set(ocel.objects[ocel.objects['ocel:type'] == 'employees']['ocel:oid'])
    filtered_events = [event for event in ocel.events if any(rel['ocel:oid'] in customer_ids for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']) and any(rel['ocel:oid'] in employee_ids for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid'])]
    filtered_ocel = pm4py.objects.ocel.obj.OCEL(events=filtered_events, objects=ocel.objects, relations=ocel.relations)
    
    # Step 2: Flatten the restricted OCEL using customers as the case notion
    flattened_ocel = pm4py.ocel_flattening(filtered_ocel, object_type='customers')
    
    # Step 3: Select the top 20% variants by frequency
    log_df = pm4py.convert_to_dataframe(flattened_ocel)
    variant_counts = log_df['case:concept:name'].value_counts()
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()
    top_variant_log = log_df[log_df['case:concept:name'].isin(top_variants)]
    
    # Step 4: Discover a Petri net from the top-variant subset
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(top_variant_log)
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    
    # Step 5: Run token-based replay
    fitness = pm4py.fitness_token_based_replay(top_variant_log, petri_net, initial_marking, final_marking)
    
    # Step 6: Report the most dominant variant among the cases that are not fit
    non_fit_cases = [case for case in top_variant_log['case:concept:name'] if case not in fitness['fit_cases']]
    non_fit_variant_counts = pd.Series(non_fit_cases).value_counts()
    dominant_non_fit_variant = non_fit_variant_counts.idxmax() if not non_fit_variant_counts.empty else None
    
    # Step 7: Calculate the share of cases whose duration exceeds the average case duration
    case_durations = top_variant_log.groupby('case:concept:name')['time:timestamp'].apply(lambda x: (x.max() - x.min()).total_seconds()).reset_index(name='duration')
    average_duration = case_durations['duration'].mean()
    exceeding_cases_ratio = (case_durations[case_durations['duration'] > average_duration].shape[0] / case_durations.shape[0]) if case_durations.shape[0] > 0 else 0.0
    
    # Step 8: Save final benchmark answer
    final_answer = {
        'dominant_non_fit_variant': dominant_non_fit_variant,
        'exceeding_cases_ratio': exceeding_cases_ratio
    }
    with open('output/benchmark_result.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/benchmark_result.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))