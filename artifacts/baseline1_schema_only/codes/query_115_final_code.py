import pm4py
import json
import os
import statistics


def main():
    ocel = ACTIVE_LOG
    
    # Step 1: Filter events linked to at least one orders object and at least one items object
    orders_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'orders']['ocel:oid'])
    items_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid'])
    filtered_events = [event for event in ocel.events if event['ocel:oid'] in orders_objects and event['ocel:oid'] in items_objects]
    filtered_ocel = pm4py.create_ocel(filtered_events, ocel.objects, ocel.relations)
    
    # Step 2: Flatten the restricted OCEL using packages as the case notion
    flattened_log = pm4py.ocel_flattening(filtered_ocel, object_type='packages')
    
    # Step 3: Select the top 20% variants by frequency
    variant_counts = flattened_log['concept:name'].value_counts()
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()
    model_building_sublog = flattened_log[flattened_log['concept:name'].isin(top_variants)]
    
    # Step 4: Discover a Petri net from that sublog
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(model_building_sublog)
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    with open('output/petri_net.pkl', 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.pkl')
    
    # Step 5: Run token-based replay and report the percentage of cases that are not fit
    replay_results = pm4py.replay_log(petri_net, initial_marking, model_building_sublog)
    case_durations = model_building_sublog.groupby('case:concept:name')['time:timestamp'].apply(lambda x: (x.max() - x.min()).total_seconds())
    average_duration = statistics.mean(case_durations)
    not_fit_cases = [case for case in replay_results if not case['fit'] and case['duration'] > average_duration]
    not_fit_percentage = (len(not_fit_cases) / len(replay_results)) * 100 if replay_results else 0
    
    # Step 6: Save final benchmark answer
    final_answer = {
        'not_fit_percentage': not_fit_percentage,
        'average_case_duration': average_duration,
        'total_cases': len(replay_results),
        'top_variants': top_variants
    }
    with open('output/benchmark_results.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/benchmark_results.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))