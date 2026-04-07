def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter events linked to at least one orders object and at least one items object
    orders_objects = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'orders'}
    items_objects = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'items'}
    filtered_events = [event for event in ocel.events if any(rel['ocel:oid'] in orders_objects for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']) and any(rel['ocel:oid'] in items_objects for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid'])]
    filtered_ocel = {
        'events': filtered_events,
        'objects': ocel.objects,
        'relations': ocel.relations
    }
    # Step 2: Discover OC-DFG on the restricted OCEL
    ocdfg = pm4py.discover_ocdfg(filtered_ocel)
    # Step 3: Prepare edge counts
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
    top_rows = sorted(top_rows, key=lambda row: row['count'], reverse=True)[:10]
    # Save edge counts to CSV
    edge_counts_df = pd.DataFrame(top_rows)
    edge_counts_df.to_csv('output/ocdfg_edge_counts.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/ocdfg_edge_counts.csv')
    # Step 4: Prepare final answer
    total_nodes = len(ocdfg['activities'])
    total_edges = sum(len(edge_map) for edge_map in edge_tables.values())
    final_answer = {
        'graph_type': 'ocdfg',
        'total_nodes': total_nodes,
        'total_edges': total_edges,
        'top_edges': top_rows,
    }
    with open('output/ocdfg.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/ocdfg.json')
    print(json.dumps(final_answer, ensure_ascii=False))