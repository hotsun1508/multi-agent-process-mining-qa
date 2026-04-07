def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter the OCEL to events linked to at least one orders object and at least one items object
    orders_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'orders']['ocel:oid'])
    items_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid'])
    filtered_events = []
    for event in ocel.events:
        linked_orders = {relation['ocel:oid'] for relation in ocel.relations if relation['ocel:eid'] == event['ocel:eid'] and relation['ocel:type'] == 'orders'}
        linked_items = {relation['ocel:oid'] for relation in ocel.relations if relation['ocel:eid'] == event['ocel:eid'] and relation['ocel:type'] == 'items'}
        if linked_orders and linked_items:
            filtered_events.append(event)
    filtered_ocel = {
        'events': filtered_events,
        'objects': ocel.objects,
        'relations': ocel.relations
    }
    
    # Step 2: Flatten the restricted OCEL using orders as the case notion
    flattened_ocel = pm4py.ocel_flattening(filtered_ocel, object_type='orders')
    
    # Step 3: Report the number of unique variants in the restricted flattened view
    unique_variants = flattened_ocel.groupby(['case:concept:name', 'concept:name']).size().reset_index(name='counts')
    variant_count = unique_variants['case:concept:name'].nunique()
    unique_variants.to_csv('output/variants_orders_items_orders.csv', index=False)
    
    # Step 4: Write the final benchmark answer to the result CSV/log
    final_answer = {'unique_variants_count': variant_count}
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    
    print('OUTPUT_FILE_LOCATION: output/variants_orders_items_orders.csv')
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    print(json.dumps(final_answer, ensure_ascii=False))