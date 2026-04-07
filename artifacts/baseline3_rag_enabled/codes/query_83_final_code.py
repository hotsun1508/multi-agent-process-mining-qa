import os
import pandas as pd
import json
import pm4py

def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Flatten the OCEL for the object type 'packages'
    flattened_log = pm4py.ocel_flattening(ocel, 'packages')
    
    # Step 2: Count the frequency of each variant
    variant_counts = flattened_log['case:concept:name'].value_counts()
    
    # Step 3: Select the top 20% variants by frequency
    top_20_percent_count = max(1, int(len(variant_counts) * 0.2))
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()
    
    # Step 4: Filter the flattened log for the top variants
    filtered_log = flattened_log[flattened_log['case:concept:name'].isin(top_variants)]
    
    # Step 5: Discover the DFG
    dfg, start_activities, end_activities = pm4py.discover_dfg(filtered_log)
    
    # Step 6: Save the DFG to CSV and PNG
    dfg_df = pd.DataFrame(list(dfg.items()), columns=['source', 'target', 'count'])
    dfg_df.to_csv(os.path.join(output_dir, 'dfg_top20_packages.csv'), index=False)
    
    # Save DFG visualization
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, os.path.join(output_dir, 'dfg_top20_packages.png'))
    
    # Step 7: Prepare final answer
    total_edges = sum(dfg.values())
    top_edges = [{'source': src, 'target': dst, 'count': count, 'share': (count / total_edges if total_edges else 0.0)} for (src, dst), count in sorted(dfg.items(), key=lambda x: x[1], reverse=True)[:10]]
    final_answer = {'top_edges': top_edges}
    
    # Save final answer to JSON
    with open(os.path.join(output_dir, 'final_answer.json'), 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'dfg_top20_packages.csv')}')
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'dfg_top20_packages.png')}')
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'final_answer.json')}')
    print(json.dumps(final_answer, ensure_ascii=False)