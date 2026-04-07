def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter events linked to at least one orders object and at least one items object
    orders_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'orders']['ocel:oid'])
    items_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid'])
    filtered_events = set()
    for relation in ocel.relations:
        if relation['ocel:oid'] in orders_objects and relation['ocel:oid'] in items_objects:
            filtered_events.add(relation['ocel:eid'])
    restricted_ocel = ocel.filter_events(list(filtered_events))

    # Step 2: Discover OC-DFG on the restricted OCEL
    ocdfg = pm4py.discover_ocdfg(restricted_ocel)
    ocdfg_path = 'output/ocdfg_visualization.png'
    pm4py.save_vis_ocdfg(ocdfg, ocdfg_path, annotation='frequency')
    print(f'OUTPUT_FILE_LOCATION: {ocdfg_path}')  

    # Step 3: Flatten the restricted OCEL to packages view
    flattened_packages = pm4py.ocel_flattening(restricted_ocel, object_type='packages')
    flattened_df = pm4py.convert_to_dataframe(flattened_packages)

    # Step 4: Calculate unique variants and share of cases exceeding average duration
    unique_variants = flattened_df['concept:name'].nunique()
    case_durations = flattened_df.groupby('case:concept:name')['time:timestamp'].max() - flattened_df.groupby('case:concept:name')['time:timestamp'].min()
    average_duration = case_durations.mean() if not case_durations.empty else 0
    cases_exceeding_average = case_durations[case_durations > average_duration]
    share_exceeding_average = len(cases_exceeding_average) / len(case_durations) if len(case_durations) > 0 else 0

    # Step 5: Save results
    final_answer = {
        'unique_variants': unique_variants,
        'share_exceeding_average': share_exceeding_average
    }
    with open('output/results.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/results.json')

    print(json.dumps(final_answer, ensure_ascii=False))