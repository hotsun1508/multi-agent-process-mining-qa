def main():
    ocel = ACTIVE_LOG
    ocdfg = pm4py.discover_ocdfg(ocel)
    path_png = 'output/ocdfg.png'
    pm4py.save_vis_ocdfg(ocdfg, path_png, annotation='frequency')
    print(f'OUTPUT_FILE_LOCATION: {path_png}')
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
    import pandas as pd
    top_edges_df = pd.DataFrame(top_rows)
    top_edges_df.to_csv('output/ocdfg_top_edges.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/ocdfg_top_edges.csv')
    print(json.dumps(final_answer, ensure_ascii=False))