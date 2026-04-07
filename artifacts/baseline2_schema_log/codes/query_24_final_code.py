import pm4py
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log)
    
    # Get the top-3 most frequent variants
    variant_counts = log_df.groupby(['case:concept:name'])['concept:name'].apply(list).value_counts().head(3)
    top_variants = variant_counts.index.tolist()
    
    # Filter the log for the top-3 variants
    filtered_log = log_df[log_df.groupby(['case:concept:name'])['concept:name'].transform(lambda x: list(x) in top_variants)]
    
    # Discover the Petri net using the Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log)
    
    # Save the Petri net visualization
    petri_net_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, petri_net_path)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_path}')  
    
    # Count places, transitions, and arcs
    places_count = len(petri_net.places)
    transitions_count = len(petri_net.transitions)
    arcs_count = len(petri_net.arcs)
    
    # Prepare the final answer
    final_answer = {
        'places': places_count,
        'transitions': transitions_count,
        'arcs': arcs_count
    }
    
    # Save the final answer to a JSON file
    answer_path = 'output/final_answer.json'
    with open(answer_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {answer_path}')  
    
    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))