def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter events linked to at least one orders object and at least one items object
    orders_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'orders']['ocel:oid'])
    items_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid'])
    filtered_events = [event for event in ocel.events if event['ocel:oid'] in orders_objects and event['ocel:oid'] in items_objects]
    filtered_ocel = {
        'ocel:events': filtered_events,
        'ocel:objects': ocel.objects,
        'ocel:relations': ocel.relations
    }
    
    # Step 2: Flatten the restricted OCEL using packages as the case notion
    flattened_log = pm4py.ocel_flattening(filtered_ocel, object_type='packages')
    
    # Step 3: Select the top 20% variants by frequency
    variant_counts = flattened_log['concept:name'].value_counts()
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()
    model_building_sublog = flattened_log[flattened_log['concept:name'].isin(top_variants)]
    
    # Step 4: Discover a Petri net from that sublog
    petri_net = pm4py.discover_petri_net_inductive(model_building_sublog)
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    
    # Step 5: Run token-based replay and report the percentage of cases that are not fit
    token_based_replay = pm4py.replay_fitness(petri_net, model_building_sublog)
    average_case_duration = model_building_sublog['time:timestamp'].max() - model_building_sublog['time:timestamp'].min()
    non_fit_cases = [case for case in token_based_replay['log_fitness'] if not case['fit'] and case['duration'] > average_case_duration]
    non_fit_percentage = len(non_fit_cases) / len(model_building_sublog) * 100
    
    # Step 6: Save outputs
    final_answer = {
        'non_fit_percentage': non_fit_percentage,
        'total_cases': len(model_building_sublog),
        'top_variants': top_variants
    }
    with open('output/final_benchmark.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_benchmark.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))