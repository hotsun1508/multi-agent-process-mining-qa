def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter the OCEL to events linked to at least one orders and one items object
    orders_objects = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'orders'}
    items_objects = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'items'}
    filtered_events = [event for event in ocel.events if any(rel['ocel:oid'] in orders_objects for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']) and any(rel['ocel:oid'] in items_objects for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid'])]
    filtered_ocel = pm4py.create_ocel(filtered_events, ocel.objects, ocel.relations)
    
    # Step 2: Discover OC-DFG on the restricted OCEL
    ocdfg = pm4py.discover_ocdfg(filtered_ocel)
    ocdfg_path = 'output/ocdfg_visualization.png'
    pm4py.save_vis_ocdfg(ocdfg, ocdfg_path, annotation='frequency')
    print(f'OUTPUT_FILE_LOCATION: {ocdfg_path}')  
    
    # Step 3: Flatten the restricted OCEL to packages view
    flattened_packages = pm4py.ocel_flattening(filtered_ocel, 'packages')
    
    # Step 4: Calculate unique variants and share of cases exceeding average duration
    case_durations = flattened_packages.groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).reset_index()
    average_duration = case_durations['time:timestamp'].mean()
    unique_variants = flattened_packages['concept:name'].nunique()
    cases_exceeding_average = (case_durations['time:timestamp'] > average_duration).mean()
    
    # Step 5: Prepare final answer
    final_answer = {
        'object_interaction': {'ocdfg': ocdfg},
        'process_discovery': {'ocdfg_path': ocdfg_path},
        'behavior_variant': {'unique_variants': unique_variants},
        'performance': {'share_exceeding_average': cases_exceeding_average}
    }
    
    # Save final answer
    with open('output/final_benchmark_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_benchmark_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))