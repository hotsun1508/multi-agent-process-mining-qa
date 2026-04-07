import pm4py
import json
import os
import statistics
from pm4py.objects.ocel import ocel
from pm4py.algo.discovery import petri as pm4py_discover_petri
from pm4py.algo.discovery import variants as pm4py_discover_variants
from pm4py.algo.filtering import variants as pm4py_filter_variants
from pm4py.algo.conformance import token_based_replay as pm4py_replay


def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter OCEL to events linked to orders and customers
    orders_objects = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'orders'}
    customers_objects = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'customers'}
    filtered_events = [event for event in ocel.events if event['ocel:oid'] in orders_objects or event['ocel:oid'] in customers_objects]
    filtered_ocel = ocel.Ocel(events=filtered_events, objects=ocel.objects, relations=ocel.relations)
    
    # Step 2: Flatten the restricted OCEL using customers as the case notion
    flattened_ocel = pm4py.ocel_flattening(filtered_ocel, object_type='customers')
    
    # Step 3: Select the top 20% variants by frequency
    variants = pm4py_discover_variants.get_variants(flattened_ocel)
    total_variants = len(variants)
    top_variants_count = int(total_variants * 0.2)
    top_variants = sorted(variants.items(), key=lambda x: x[1], reverse=True)[:top_variants_count]
    
    # Step 4: Report the share of cases in that top-variant subset whose case duration exceeds the average case duration
    case_durations = [case['duration'] for case in flattened_ocel]
    average_duration = statistics.mean(case_durations)
    top_variant_cases = [case for case in flattened_ocel if case['variant'] in top_variants]
    exceeding_cases = sum(1 for case in top_variant_cases if case['duration'] > average_duration)
    share_exceeding = exceeding_cases / len(top_variant_cases) if top_variant_cases else 0
    
    # Step 5: Discover a Petri net from that top-variant subset
    top_variant_flattened = pm4py_filter_variants.filter_variants(flattened_ocel, top_variants)
    petri_net = pm4py_discover_petri.discover_petri_net(top_variant_flattened)
    
    # Step 6: Compute token-based replay fitness of that model on the full restricted flattened view
    fitness = pm4py_replay.apply(petri_net, flattened_ocel)
    
    # Save outputs
    os.makedirs('output', exist_ok=True)
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    
    final_answer = {
        'share_exceeding_cases': share_exceeding,
        'fitness': fitness
    }
    with open('output/benchmark_result.json', 'w') as f:
        json.dump(final_answer, f)
    print('OUTPUT_FILE_LOCATION: output/benchmark_result.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))