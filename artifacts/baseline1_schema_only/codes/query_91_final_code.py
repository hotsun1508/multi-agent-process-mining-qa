import pm4py
import json
import os
import pickle


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the packages view
    flat_packages = pm4py.ocel_flattening(ocel, 'packages')
    
    # Get the frequency of each variant
    variant_counts = flat_packages['concept:name'].value_counts()
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()
    
    # Create a sublog with only the top 20% variants
    sublog = flat_packages[flat_packages['concept:name'].isin(top_variants)]
    
    # Discover a Petri net from the sublog
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(sublog)
    
    # Save the Petri net model
    with open('output/model_top20.pkl', 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)
    print('OUTPUT_FILE_LOCATION: output/model_top20.pkl')
    
    # Compute token-based replay fitness on the full flattened packages view
    fitness = pm4py.fitness_token_based_replay(petri_net, initial_marking, flat_packages)
    
    # Save the fitness results
    with open('output/fitness_full.json', 'w') as f:
        json.dump(fitness, f)
    print('OUTPUT_FILE_LOCATION: output/fitness_full.json')
    
    # Prepare the final answer
    final_answer = {
        'behavior_variant': {'top_variants': top_variants},
        'process_discovery': {'model': 'model_top20.pkl'},
        'conformance': {'fitness': 'fitness_full.json'}
    }
    print(json.dumps(final_answer, ensure_ascii=False))