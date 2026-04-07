import pm4py
import pandas as pd
import json
import os


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for orders
    flat_orders = pm4py.ocel_flattening(ocel, 'orders')
    # Discover the Directly-Follows Graph (DFG)
    dfg, start_activities, end_activities = pm4py.discover_dfg(flat_orders)
    # Save DFG visualization
    dfg_png_path = 'output/dfg_orders.png'
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_png_path)
    print(f'OUTPUT_FILE_LOCATION: {dfg_png_path}')  
    # Prepare DFG edges for CSV
    dfg_edges = pd.DataFrame(dfg.items(), columns=['edge', 'frequency'])
    dfg_edges.to_csv('output/dfg_edges_orders.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/dfg_edges_orders.csv')
    # Prepare final answer
    total_edges = sum(dfg.values()) if dfg else 0
    top_edges = [
        {'source': src, 'target': dst, 'count': count, 'share': (count / total_edges if total_edges else 0.0)}
        for (src, dst), count in sorted(dfg.items(), key=lambda x: x[1], reverse=True)
    ]
    final_answer = {'top_edges': top_edges}
    print(json.dumps(final_answer, ensure_ascii=False))