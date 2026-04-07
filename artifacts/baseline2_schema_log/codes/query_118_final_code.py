import pm4py
import json
import os
import statistics
from collections import Counter

def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter the raw OCEL to events linked to at least one customer and one employee
    customer_ids = set(ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid'])
    employee_ids = set(ocel.objects[ocel.objects['ocel:type'] == 'employees']['ocel:oid'])
    filtered_events = [event for event in ocel.events if any(rel['ocel:oid'] in customer_ids for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']) and any(rel['ocel:oid'] in employee_ids for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid'])]
    filtered_ocel = pm4py.create_ocel(filtered_events, ocel.objects, ocel.relations)
    
    # Step 2: Flatten the restricted OCEL using customers as the case notion
    flattened_log = pm4py.ocel_flattening(filtered_ocel, object_type='customers')
    
    # Step 3: Select the top 20% variants by frequency
    variant_counts = Counter(flattened_log['concept:name'])
    total_variants = len(variant_counts)
    top_20_percent_count = int(total_variants * 0.2)
    top_variants = variant_counts.most_common(top_20_percent_count)
    top_variant_names = [variant[0] for variant in top_variants]
    top_flattened_log = flattened_log[flattened_log['concept:name'].isin(top_variant_names)]
    
    # Step 4: Discover a Petri net from that top-20%-variant subset
    petri_net = pm4py.discover_petri_net_inductive(top_flattened_log)
    
    # Step 5: Run token-based replay
    fitness = pm4py.evaluate_replay_fitness(top_flattened_log, petri_net)
    
    # Step 6: Report the most dominant variant among the cases that are not fit
    non_fit_cases = top_flattened_log[top_flattened_log['replay_fitness'] < 1.0]
    non_fit_variant_counts = Counter(non_fit_cases['concept:name'])
    most_dominant_non_fit_variant = non_fit_variant_counts.most_common(1)
    
    # Step 7: Calculate the share of cases whose case duration exceeds the average case duration
    case_durations = top_flattened_log.groupby('case:concept:name')['time:timestamp'].apply(lambda x: (x.max() - x.min()).total_seconds()).tolist()
    average_duration = statistics.mean(case_durations)
    exceeding_cases = sum(1 for duration in case_durations if duration > average_duration)
    share_exceeding_cases = exceeding_cases / len(case_durations) if case_durations else 0
    
    # Save outputs
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    
    final_answer = {
        'most_dominant_non_fit_variant': most_dominant_non_fit_variant,
        'share_exceeding_cases': share_exceeding_cases
    }
    
    with open('output/results.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/results.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))