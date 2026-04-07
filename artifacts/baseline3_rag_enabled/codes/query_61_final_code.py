import pm4py
import json
import os


def main():
    ocel = ACTIVE_LOG
    
    # Discover the OCDFG
    ocdfg = pm4py.discover_ocdfg(ocel)
    
    # Prepare output directory
    os.makedirs('output', exist_ok=True)
    
    # Save OCDFG visualization
    png_path = 'output/ocdfg.png'
    pm4py.save_vis_ocdfg(ocdfg, png_path, annotation='frequency')
    print(f'OUTPUT_FILE_LOCATION: {png_path}')
    
    # Prepare top edges
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
    
    # Save top edges to CSV
    import pandas as pd
    top_edges_df = pd.DataFrame(top_rows)
    top_edges_csv_path = 'output/ocdfg_top_edges.csv'
    top_edges_df.to_csv(top_edges_csv_path, index=False)
    print(f'OUTPUT_FILE_LOCATION: {top_edges_csv_path}')
    
    # Prepare final answer
    total_nodes = len(ocdfg['activities'])
    total_edges = sum(len(edge_map) for edge_map in edge_tables.values())
    final_answer = {
        'graph_type': 'ocdfg',
        'total_nodes': total_nodes,
        'total_edges': total_edges,
        'top_edges': top_rows,
    }
    
    # Save final answer to JSON
    json_path = 'output/ocdfg.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {json_path}')
    
    print(json.dumps(final_answer, ensure_ascii=False))