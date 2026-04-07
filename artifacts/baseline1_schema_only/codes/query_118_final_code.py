import pm4py
import json
import os
import statistics
from collections import Counter


def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter events linked to at least one customer and one employee
    customer_ids = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'customers'}
    employee_ids = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'employees'}
    filtered_events = [event for event in ocel.events if any(rel['ocel:oid'] in customer_ids for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']) and any(rel['ocel:oid'] in employee_ids for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid'])]
    filtered_ocel = pm4py.create_ocel(filtered_events, ocel.objects, ocel.relations)
    
    # Step 2: Flatten the restricted OCEL using customers as the case notion
    flattened_log = pm4py.ocel_flattening(filtered_ocel, 'customers')
    
    # Step 3: Select the top 20% variants by frequency
    variant_counts = Counter(tuple(row['ocel:activity'] for row in flattened_log))
    total_variants = sum(variant_counts.values())
    top_20_percent_count = int(total_variants * 0.2)
    top_variants = variant_counts.most_common(top_20_percent_count)
    top_variant_set = set(variant for variant, _ in top_variants)
    
    # Filter the flattened log to only include top variants
    top_flattened_log = [row for row in flattened_log if tuple(row['ocel:activity']) in top_variant_set]
    
    # Step 4: Discover a Petri net from the top-20%-variant subset
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(top_flattened_log)
    
    # Save the Petri net visualization
    petri_net_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(petri_net, petri_net_path)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_path}')  
    
    # Step 5: Run token-based replay
    fitness, unfit_traces = pm4py.token_based_replay(top_flattened_log, petri_net, initial_marking, final_marking)
    
    # Analyze unfit traces
    unfit_variant_counts = Counter(tuple(row['ocel:activity'] for row in trace) for trace in unfit_traces)
    dominant_unfit_variant = unfit_variant_counts.most_common(1)[0] if unfit_variant_counts else None
    
    # Calculate average case duration
    case_durations = [sum(row['ocel:timestamp'] for row in trace) for trace in top_flattened_log]
    average_duration = statistics.mean(case_durations) if case_durations else 0
    
    # Calculate share of cases exceeding average duration
    exceeding_cases = sum(1 for duration in case_durations if duration > average_duration)
    share_exceeding = exceeding_cases / len(case_durations) if case_durations else 0
    
    # Prepare final answer
    final_answer = {
        'dominant_unfit_variant': dominant_unfit_variant,
        'share_exceeding_average_duration': share_exceeding
    }
    
    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))