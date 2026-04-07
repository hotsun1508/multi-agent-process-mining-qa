import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    # Step 1: Convert the event log to a case dataframe
    df = pm4py.convert_to_dataframe(event_log)
    # Step 2: Filter cases with variants occurring exactly once
    variant_counts = df.groupby('case:concept:name')['concept:name'].count()
    unique_variants = variant_counts[variant_counts == 1].index.tolist()
    filtered_df = df[df['case:concept:name'].isin(unique_variants)]
    # Step 3: Discover the Directly-Follows Graph (DFG)
    dfg, start_activities, end_activities = pm4py.discover_dfg(filtered_df)
    # Step 4: Identify the most frequent edge in the DFG
    most_frequent_edge = max(dfg.items(), key=lambda x: x[1])
    source_activity, target_activity = most_frequent_edge[0]
    # Step 5: Filter the original dataframe for the most frequent edge
    edge_df = filtered_df[(filtered_df['concept:name'] == source_activity) | (filtered_df['concept:name'] == target_activity)]
    # Step 6: Count resources on the filtered edge
    resource_counts = edge_df[edge_df['concept:name'] == source_activity]['org:resource'].value_counts()
    # Step 7: Get the top 5 resources
    top_resources = resource_counts.head(5).to_dict()
    # Step 8: Prepare the final output dictionary
    final_answer = {
        'most_frequent_edge': {'source': source_activity, 'target': target_activity},
        'top_resources': top_resources
    }
    # Step 9: Save the DFG visualization
    png_path = 'output/dfg_visualization.png'
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, png_path)
    print(f'OUTPUT_FILE_LOCATION: {png_path}')  
    # Step 10: Save the final answer as JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    print(json.dumps(final_answer, ensure_ascii=False))