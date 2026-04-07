def main():
    ocel = ACTIVE_LOG
    flattened_orders = pm4py.ocel_flattening(ocel, object_type='orders')
    dfg, start_activities, end_activities = pm4py.discover_dfg(flattened_orders)
    png_path = 'output/dfg_orders.png'
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, png_path)
    print(f'OUTPUT_FILE_LOCATION: {png_path}')
    edge_counts = [(f'{src} -> {dst}', count) for (src, dst), count in dfg.items()]
    dfg_edges_df = pd.DataFrame(edge_counts, columns=['edge', 'frequency'])
    dfg_edges_path = 'output/dfg_edges_orders.csv'
    dfg_edges_df.to_csv(dfg_edges_path, index=False)
    print(f'OUTPUT_FILE_LOCATION: {dfg_edges_path}')
    total_edges = sum(dfg.values()) if dfg else 0
    top_edges = [{'source': src, 'target': dst, 'count': count, 'share': (count / total_edges if total_edges else 0.0)} for (src, dst), count in sorted(dfg.items(), key=lambda x: x[1], reverse=True)[:10]]
    final_answer = {'top_edges': top_edges}
    print(json.dumps(final_answer, ensure_ascii=False))