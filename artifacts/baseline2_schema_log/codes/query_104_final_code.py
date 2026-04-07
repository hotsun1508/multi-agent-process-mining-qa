def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to items view
    flattened_items = pm4py.ocel_flattening(ocel, 'items')
    # Calculate case durations
    case_durations = flattened_items.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = case_durations['max'] - case_durations['min']
    average_duration = case_durations['duration'].mean()
    # Isolate delayed cases
    delayed_cases = case_durations[case_durations['duration'] > average_duration].index.tolist()
    # Map delayed cases back to raw OCEL events
    delayed_events = []
    for case in delayed_cases:
        case_events = ocel.events[ocel.events['ocel:oid'].isin(ocel.relations[ocel.relations['ocel:oid'].isin(ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid'])]['ocel:oid']) & (ocel.events['case:concept:name'] == case)]
        delayed_events.append(case_events)
    delayed_events = pd.concat(delayed_events)
    # Keep only events linked to items and customers
    customer_ids = ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid']
    linked_events = delayed_events[delayed_events['ocel:oid'].isin(customer_ids)]
    # Propagate the filter
    filtered_ocel = ocel
    filtered_ocel.events = linked_events
    # Flatten the resulting restricted OCEL using items as the case notion
    restricted_flattened = pm4py.ocel_flattening(filtered_ocel, 'items')
    # Discover the most dominant variant
    variant_counts = restricted_flattened['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    # Discover DFG on the cases of that dominant variant
    dfg, start_activities, end_activities = pm4py.discover_dfg(restricted_flattened[restricted_flattened['concept:name'] == dominant_variant])
    # Save DFG visualization
    dfg_path = 'output/dfg_visualization.png'
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_path)
    print(f'OUTPUT_FILE_LOCATION: {dfg_path}')  
    # Prepare final answer
    final_answer = {'dominant_variant': dominant_variant, 'average_case_duration': average_duration}
    print(json.dumps(final_answer, ensure_ascii=False))