def main():
    ocel = ACTIVE_LOG
    # Filter events linked to at least one orders object and at least one items object
    events = ocel.events
    relations = ocel.relations
    orders_ids = set(ocel.objects[ocel.objects['ocel:type'] == 'orders']['ocel:oid'])
    items_ids = set(ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid'])

    # Create a list to count linked orders and items per event
    event_link_counts = []
    for event in events:
        event_id = event['ocel:eid']
        linked_orders = set()
        linked_items = set()

        # Find linked objects for the current event
        for relation in relations:
            if relation['ocel:eid'] == event_id:
                if relation['ocel:oid'] in orders_ids:
                    linked_orders.add(relation['ocel:oid'])
                elif relation['ocel:oid'] in items_ids:
                    linked_items.add(relation['ocel:oid'])

        # Count the number of linked orders and items
        event_link_counts.append((len(linked_orders), len(linked_items)))

    # Calculate minimum and maximum observed co-occurrence pair
    min_count = min(event_link_counts, key=lambda x: (x[0], x[1]))
    max_count = max(event_link_counts, key=lambda x: (x[0], x[1]))

    final_answer = {
        'min_linked_orders': min_count[0],
        'min_linked_items': min_count[1],
        'max_linked_orders': max_count[0],
        'max_linked_items': max_count[1]
    }

    # Save the final answer to a JSON file
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')

    print(json.dumps(final_answer, ensure_ascii=False))