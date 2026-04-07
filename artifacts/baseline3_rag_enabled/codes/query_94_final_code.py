import os
import pandas as pd
import pm4py
import json

def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Retrieve event IDs linked to both 'orders' and 'items' objects
    orders_objects = [obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'orders']
    items_objects = [obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'items']
    
    event_ids = set()
    for relation in ocel.relations:
        if relation['ocel:oid'] in orders_objects or relation['ocel:oid'] in items_objects:
            event_ids.add(relation['ocel:eid'])
    
    # Step 2: Filter the OCEL to create a restricted OCEL with relevant events
    restricted_ocel = pm4py.ocel_filter(ocel, event_ids)
    
    # Step 3: Discover the OC-DFG from the restricted OCEL
    ocdfg = pm4py.discover_ocdfg(restricted_ocel)
    
    # Step 4: Extract edges from the OC-DFG and count occurrences of events for each edge
    edge_tables = ocdfg['edges']['event_couples']
    edge_counts = []
    for object_type, edge_map in edge_tables.items():
        for (src, dst), linked_pairs in edge_map.items():
            edge_counts.append({'source': src, 'target': dst, 'count': len(linked_pairs)})
    
    # Sort edges by count and select the top-10 edges
    top_edges = sorted(edge_counts, key=lambda x: x['count'], reverse=True)[:10]
    
    # Prepare a per-edge event count table
    edge_count_table = pd.DataFrame(top_edges)
    edge_count_table.to_csv(os.path.join(output_dir, 'ocdfg_edge_counts.csv'), index=False)
    
    # Step 5: Construct the final JSON-serializable dictionary
    total_edges = sum(len(edge_map) for edge_map in edge_tables.values())
    total_nodes = len(ocdfg['activities'])
    final_answer = {
        'total_edges': total_edges,
        'total_nodes': total_nodes,
        'top_edges': top_edges
    }
    
    # Save the final answer as a JSON file
    with open(os.path.join(output_dir, 'final_answer.json'), 'w') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'ocdfg_edge_counts.csv')}')
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'final_answer.json')}')
    
    print(json.dumps(final_answer, ensure_ascii=False))