def main():
    ocel = ACTIVE_LOG
    # Get events and relations
    events = ocel.events
    relations = ocel.relations

    # Filter events linked to at least one orders and one items object
    valid_events = []
    for event in events:
        linked_orders = set()
        linked_items = set()
        for relation in relations:
            if relation['ocel:eid'] == event['ocel:eid']:
                if relation['ocel:type'] == 'orders':
                    linked_orders.add(relation['ocel:oid'])
                elif relation['ocel:type'] == 'items':
                    linked_items.add(relation['ocel:oid'])
        if linked_orders and linked_items:
            valid_events.append((len(linked_orders), len(linked_items)))

    # Calculate min and max co-occurrence pairs
    if valid_events:
        min_pair = min(valid_events, key=lambda x: (x[0], x[1]))
        max_pair = max(valid_events, key=lambda x: (x[0], x[1]))
    else:
        min_pair = (0, 0)
        max_pair = (0, 0)

    # Prepare final answer
    final_answer = {
        'min_co_occurrence': min_pair,
        'max_co_occurrence': max_pair
    }

    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')

    print(json.dumps(final_answer, ensure_ascii=False))