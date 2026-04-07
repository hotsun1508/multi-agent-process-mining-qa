import os
import json
import pm4py


def main():
    ocel = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Retrieve event IDs linked to both 'orders' and 'items'
    required_object_types = ['orders', 'items']
    event_ids = set()
    for relation in ocel.relations:
        if relation['ocel:type'] in required_object_types:
            event_ids.add(relation['ocel:eid'])

    # Step 2: Filter the OCEL to retain only the events that are linked to these object types
    filtered_events = [event for event in ocel.events if event['ocel:eid'] in event_ids]

    # Step 3: Count the number of linked orders and items objects per event
    order_counts = []
    item_counts = []
    for event in filtered_events:
        linked_orders = sum(1 for relation in ocel.relations if relation['ocel:eid'] == event['ocel:eid'] and relation['ocel:type'] == 'orders')
        linked_items = sum(1 for relation in ocel.relations if relation['ocel:eid'] == event['ocel:eid'] and relation['ocel:type'] == 'items')
        order_counts.append(linked_orders)
        item_counts.append(linked_items)

    # Step 4: Calculate the minimum and maximum observed co-occurrence pair (#A, #B)
    min_co_occurrence = (min(order_counts), min(item_counts))
    max_co_occurrence = (max(order_counts), max(item_counts))

    # Step 5: Construct the final result dictionary
    final_answer = {
        'min_co_occurrence': min_co_occurrence,
        'max_co_occurrence': max_co_occurrence
    }

    # Step 6: Save the result to a JSON file
    with open(os.path.join(output_dir, 'result.json'), 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'result.json')})

    print(json.dumps(final_answer, ensure_ascii=False))