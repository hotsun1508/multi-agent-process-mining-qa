def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the items view
    flattened_items = pm4py.ocel_flattening(ocel, 'items')
    # Calculate case durations
    case_durations = flattened_items.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = case_durations['max'] - case_durations['min']
    average_duration = case_durations['duration'].mean()
    # Isolate delayed cases
    delayed_cases = case_durations[case_durations['duration'] > average_duration].index.tolist()
    # Map delayed cases back to raw OCEL events
    delayed_events = ocel.events[ocel.events['ocel:oid'].isin(ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid']) & ocel.events['case:concept:name'].isin(delayed_cases)]
    # Keep only events linked to at least one items object and at least one customers object
    items_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid'])
    customers_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid'])
    valid_events = delayed_events[delayed_events['ocel:oid'].isin(items_objects) & delayed_events['ocel:oid'].isin(customers_objects)]
    # Propagate the filter to create a restricted OCEL
    restricted_ocel = pm4py.ocel_create(ocel.events[ocel.events['ocel:eid'].isin(valid_events['ocel:eid'])], ocel.objects, ocel.relations)
    # Flatten the restricted OCEL using items as the case notion
    flattened_restricted_items = pm4py.ocel_flattening(restricted_ocel, 'items')
    # Discover the most dominant variant
    variant_counts = flattened_restricted_items['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    # Discover DFG on the cases of that dominant variant
    dfg, start_activities, end_activities = pm4py.discover_dfg(flattened_restricted_items[flattened_restricted_items['concept:name'] == dominant_variant])
    # Save DFG visualization
    dfg_path = 'output/dfg_visualization.png'
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_path)
    print(f'OUTPUT_FILE_LOCATION: {dfg_path}')  
    # Prepare final answer
    final_answer = {'dominant_variant': dominant_variant, 'average_case_duration': average_duration}
    print(json.dumps(final_answer, ensure_ascii=False))