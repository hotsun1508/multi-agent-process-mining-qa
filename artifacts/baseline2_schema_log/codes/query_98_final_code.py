def main():
    ocel = ACTIVE_LOG
    # Step 1: Flatten the OCEL to customers view
    flattened_customers = pm4py.ocel_flattening(ocel, 'customers')
    # Step 2: Calculate case durations
    case_durations = flattened_customers.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = case_durations['max'] - case_durations['min']
    average_duration = case_durations['duration'].mean()
    # Step 3: Isolate delayed cases
    delayed_cases = case_durations[case_durations['duration'] > average_duration].index.tolist()
    # Step 4: Map delayed cases back to raw OCEL events
    delayed_events = ocel.events[ocel.events['ocel:oid'].isin(ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid']) & ocel.events['case:concept:name'].isin(delayed_cases)]
    # Step 5: Filter events linked to at least one items object and at least one customers object
    item_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid'])
    customer_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid'])
    filtered_events = delayed_events[delayed_events['ocel:oid'].isin(item_objects) & delayed_events['ocel:oid'].isin(customer_objects)]
    # Step 6: Create a restricted OCEL
    restricted_ocel = {
        'events': filtered_events,
        'objects': ocel.objects,
        'relations': ocel.relations
    }
    # Step 7: Flatten the restricted OCEL again using customers as the case notion
    flattened_restricted_customers = pm4py.ocel_flattening(restricted_ocel, 'customers')
    # Step 8: Discover the most dominant variant
    variant_counts = flattened_restricted_customers['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    # Save the dominant variant to a JSON file
    with open('output/dom_variant_delayed_joint.json', 'w', encoding='utf-8') as f:
        json.dump({'dominant_variant': dominant_variant}, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/dom_variant_delayed_joint.json')
    # Final benchmark answer
    final_answer = {'dominant_variant': dominant_variant, 'average_case_duration': average_duration}
    print(json.dumps(final_answer, ensure_ascii=False))