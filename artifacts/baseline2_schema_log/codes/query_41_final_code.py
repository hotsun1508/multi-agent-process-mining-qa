import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    
    # Get the frequency of each variant
    variants = log_df.groupby(['case:concept:name'])['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    top_20_percent_count = int(len(variants) * 0.2)
    top_variants = variants.nlargest(top_20_percent_count).index.tolist()
    
    # Filter the log for top 20% variants
    top_variant_cases = log_df[log_df.groupby(['case:concept:name'])['concept:name'].apply(lambda x: ' -> '.join(x)).isin(top_variants)]
    
    # Discover Petri net from the filtered log
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(top_variant_cases)
    
    # Save the Petri net visualization
    petri_net_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, petri_net_path)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_path}')  
    
    # Perform token-based replay to find non-fit cases
    non_fit_cases = []
    for case_id, group in top_variant_cases.groupby('case:concept:name'):
        replay_result = pm4py.replay_trace(petri_net, initial_marking, group['concept:name'].tolist())
        if not replay_result['fit']:
            non_fit_cases.append(group)
    
    # Combine non-fit cases into a single DataFrame
    non_fit_cases_df = pd.concat(non_fit_cases)
    
    # Get the top 5 resources from non-fit cases
    top_resources = non_fit_cases_df['org:resource'].value_counts().nlargest(5).to_dict()
    
    # Prepare final answer
    final_answer = {
        'top_resources': top_resources,
        'petri_net_summary': {
            'places': len(petri_net.places),
            'transitions': len(petri_net.transitions),
            'arcs': len(petri_net.arcs),
        }
    }
    
    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))