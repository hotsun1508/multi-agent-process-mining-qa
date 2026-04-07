import pm4py
import json
import pickle


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the packages view
    flat_log = pm4py.ocel_flattening(ocel, object_type='packages')
    # Discover variants and their frequencies
    variants = flat_log['concept:name'].value_counts()
    top_20_percent_count = int(len(variants) * 0.2)
    top_variants = variants.nlargest(top_20_percent_count).index.tolist()
    # Filter the log for the top 20% variants
    sublog = flat_log[flat_log['concept:name'].isin(top_variants)]
    # Discover a Petri net from the sublog
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(sublog)
    # Save the Petri net model
    with open('output/model_top20.pkl', 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)
    print('OUTPUT_FILE_LOCATION: output/model_top20.pkl')
    # Compute token-based replay fitness on the full flattened packages view
    fitness = pm4py.fitness_token_based_replay(petri_net, initial_marking, final_marking, flat_log)
    # Save the fitness result
    with open('output/fitness_full.json', 'w') as f:
        json.dump(fitness, f)
    print('OUTPUT_FILE_LOCATION: output/fitness_full.json')
    # Prepare final answer
    final_answer = {'model': 'model_top20.pkl', 'fitness': 'fitness_full.json'}
    print(json.dumps(final_answer, ensure_ascii=False))