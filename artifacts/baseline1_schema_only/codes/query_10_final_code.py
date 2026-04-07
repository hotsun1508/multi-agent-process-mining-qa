import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    # Convert event log to dataframe
    log_df = pm4py.convert_to_dataframe(event_log)
    # Get the top 20% most frequent variants
    variant_counts = log_df.groupby(['case:concept:name'])['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    top_variants = variant_counts.head(int(len(variant_counts) * 0.2)).index.tolist()
    top_cases = log_df[log_df.groupby(['case:concept:name'])['concept:name'].apply(lambda x: ' -> '.join(x)).isin(top_variants)]['case:concept:name'].unique()
    filtered_log = log_df[log_df['case:concept:name'].isin(top_cases)]
    # Load the reference Petri net model (assumed to be pre-loaded)
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log)
    # Calculate token-based replay precision score
    precision_score = pm4py.conformance_token_based_replay(filtered_log, petri_net, initial_marking, final_marking)
    # Save the Petri net visualization
    png_path = 'output/petri_net_visualization.png'
    pm4py.save_vis_petri_net(petri_net, png_path)
    print(f'OUTPUT_FILE_LOCATION: {png_path}')  
    # Prepare final answer
    final_answer = {'token_based_replay_precision': precision_score}
    # Save final answer to JSON
    with open('output/precision_score.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/precision_score.json')
    print(json.dumps(final_answer, ensure_ascii=False))