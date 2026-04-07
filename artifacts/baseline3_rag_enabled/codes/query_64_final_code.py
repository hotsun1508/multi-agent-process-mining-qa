import os
import pandas as pd
import json
import pm4py

def main():
    ocel = ACTIVE_LOG
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL for the object type 'orders'
    flattened_log = pm4py.ocel_flattening(ocel, 'orders')  # Flattening the OCEL

    # Step 2: Discover the Directly-Follows Graph (DFG)
    dfg = pm4py.discover_dfg(flattened_log)

    # Step 3: Save DFG edges to CSV
    dfg_edges = pd.DataFrame(dfg.items(), columns=['edge', 'frequency'])
    dfg_edges.to_csv(os.path.join(output_dir, 'dfg_edges_orders.csv'), index=False)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'dfg_edges_orders.csv')}')

    # Step 4: Save DFG visualization
    dfg_png_path = os.path.join(output_dir, 'dfg_orders.png')
    pm4py.save_vis_dfg(dfg, dfg_png_path)
    print(f'OUTPUT_FILE_LOCATION: {dfg_png_path}')

    # Step 5: Prepare final benchmark answer
    total_edges = len(dfg)
    final_answer = {
        'total_edges': total_edges,
        'dfg_edges_count': len(dfg_edges),
        'dfg_visualization': dfg_png_path
    }
    print(json.dumps(final_answer, ensure_ascii=False))