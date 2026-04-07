def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter events linked to at least one orders and one customers object
    orders_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'orders']['ocel:oid'])
    customers_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid'])
    filtered_events = [event for event in ocel.events if any(rel['ocel:oid'] in orders_objects for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']) and any(rel['ocel:oid'] in customers_objects for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid'])]
    filtered_ocel = {
        'events': filtered_events,
        'objects': ocel.objects,
        'relations': ocel.relations
    }
    
    # Step 2: Flatten the restricted OCEL using orders as the case notion
    flattened_log = pm4py.ocel_flattening(filtered_ocel, 'orders')
    
    # Step 3: Find the most dominant variant
    variant_counts = flattened_log['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    dominant_variant_count = variant_counts.max()
    
    # Step 4: Discover DFG on the cases of that dominant variant
    filtered_dominant_cases = flattened_log[flattened_log['concept:name'] == dominant_variant]
    dfg, start_activities, end_activities = pm4py.discover_dfg(filtered_dominant_cases)
    dfg_path = 'output/dfg_visualization.png'
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_path)
    print(f'OUTPUT_FILE_LOCATION: {dfg_path}')  
    
    # Step 5: Calculate average case duration
    average_duration = (filtered_dominant_cases['time:timestamp'].max() - filtered_dominant_cases['time:timestamp'].min()).total_seconds() / len(filtered_dominant_cases['case:concept:name'].unique())
    
    # Final answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'dominant_variant_count': dominant_variant_count,
        'average_case_duration_seconds': average_duration
    }
    print(json.dumps(final_answer, ensure_ascii=False))

    # Save final answer
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')