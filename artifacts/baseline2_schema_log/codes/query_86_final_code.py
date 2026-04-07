def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter events linked to at least one items object and at least one customers object
    items_objects = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'items'}
    customers_objects = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'customers'}

    # Step 2: Find events linked to these objects
    filtered_events = set()
    for relation in ocel.relations:
        if relation['ocel:oid'] in items_objects or relation['ocel:oid'] in customers_objects:
            filtered_events.add(relation['ocel:eid'])

    # Step 3: Create a restricted OCEL
    restricted_ocel = {
        'events': [event for event in ocel.events if event['ocel:eid'] in filtered_events],
        'objects': ocel.objects,
        'relations': [relation for relation in ocel.relations if relation['ocel:eid'] in filtered_events]
    }

    # Step 4: Discover OC-DFG on the restricted OCEL
    ocdfg = pm4py.discover_ocdfg(restricted_ocel)

    # Step 5: Save OC-DFG visualization
    png_path = 'output/ocdfg_items_customers.png'
    pm4py.save_vis_ocdfg(ocdfg, png_path, annotation='frequency')
    print(f'OUTPUT_FILE_LOCATION: {png_path}')  

    # Step 6: Prepare JSON-serializable summary
    edge_tables = ocdfg['edges']['event_couples']
    top_rows = []
    for object_type, edge_map in edge_tables.items():
        for (src, dst), linked_pairs in edge_map.items():
            top_rows.append({
                'object_type': object_type,
                'source': src,
                'target': dst,
                'count': len(linked_pairs),
            })
    total_nodes = len(ocdfg['activities'])
    total_edges = sum(len(edge_map) for edge_map in edge_tables.values())
    final_answer = {
        'graph_type': 'ocdfg',
        'total_nodes': total_nodes,
        'total_edges': total_edges,
        'top_edges': sorted(top_rows, key=lambda row: row['count'], reverse=True)[:10],
    }

    # Step 7: Save OC-DFG summary to JSON
    with open('output/ocdfg_items_customers.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/ocdfg_items_customers.json')

    # Final output
    print(json.dumps(final_answer, ensure_ascii=False))