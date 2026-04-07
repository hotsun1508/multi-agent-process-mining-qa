import os
import json
import pm4py

def main():
    ocel = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Filter events linked to 'items' and 'customers'
    items_objects = ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid']
    customers_objects = ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid']
    
    items_relations = ocel.relations[ocel.relations['ocel:oid'].isin(items_objects)]
    customers_relations = ocel.relations[ocel.relations['ocel:oid'].isin(customers_objects)]
    
    # Get event IDs linked to both items and customers
    event_ids = set(items_relations['ocel:eid']).intersection(set(customers_relations['ocel:eid']))
    
    # Step 2: Create a restricted OCEL
    restricted_ocel = pm4py.objects.ocel.obj.OCEL(
        events=ocel.events[ocel.events['ocel:eid'].isin(event_ids)],
        objects=ocel.objects,
        relations=ocel.relations[ocel.relations['ocel:eid'].isin(event_ids)]
    )

    # Step 3: Discover the OC-DFG from the restricted OCEL
    ocdfg = pm4py.discover_ocdfg(restricted_ocel)

    # Step 4: Save the OC-DFG visualization
    png_path = os.path.join(output_dir, 'ocdfg_items_customers.png')
    pm4py.save_vis_ocdfg(ocdfg, png_path, annotation='frequency')
    print(f'OUTPUT_FILE_LOCATION: {png_path}')  

    # Step 5: Save the OC-DFG summary as JSON
    json_path = os.path.join(output_dir, 'ocdfg_items_customers.json')
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
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {json_path}')  

    print(json.dumps(final_answer, ensure_ascii=False))