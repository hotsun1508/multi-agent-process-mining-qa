def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter the OCEL to events linked to at least one orders object and at least one items object
    orders_objects = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'orders'}
    items_objects = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'items'}
    filtered_events = [event for event in ocel.events if any(rel['ocel:oid'] in orders_objects for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']) and any(rel['ocel:oid'] in items_objects for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid'])]
    filtered_ocel = {
        'ocel:events': filtered_events,
        'ocel:objects': ocel.objects,
        'ocel:relations': ocel.relations
    }
    # Step 2: Flatten the restricted OCEL using orders as the case notion
    flattened_log = pm4py.ocel_flattening(filtered_ocel, object_type='orders')
    # Step 3: Calculate the number of unique variants
    unique_variants = flattened_log['concept:name'].nunique()
    # Step 4: Save the variants to a CSV file
    variants_df = flattened_log.groupby('concept:name').size().reset_index(name='count')
    variants_df.to_csv('output/variants_orders_items_orders.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/variants_orders_items_orders.csv')
    # Step 5: Prepare the final answer
    final_answer = {'unique_variants': unique_variants}
    print(json.dumps(final_answer, ensure_ascii=False))