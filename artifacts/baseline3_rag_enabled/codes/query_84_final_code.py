import os
import json
import pm4py
import pickle

def main():
    ocel = ACTIVE_LOG
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL for the object type 'items'
    flattened_log = pm4py.ocel_flattening(ocel, 'items')

    # Step 2: Discover a Petri net using the Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(flattened_log)

    # Step 3: Save the discovered Petri net
    petri_net_path = 'output/im_items.pkl'
    with open(petri_net_path, 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_path}')  

    # Step 4: Compute token-based replay fitness
    fitness = pm4py.fitness_token_based_replay(flattened_log, petri_net, initial_marking, final_marking)

    # Step 5: Save the fitness results to a JSON file
    fitness_path = 'output/fitness_im_items.json'
    with open(fitness_path, 'w') as f:
        json.dump(fitness, f)
    print(f'OUTPUT_FILE_LOCATION: {fitness_path}')  

    # Step 6: Prepare the final benchmark answer
    final_answer = {
        'status': 'success',
        'result_type': 'composite',
        'view': 'raw_ocel_or_flattened_view_as_specified',
        'result': {
            'fitness': fitness
        },
        'artifacts': ['im_items.pkl', 'fitness_im_items.json']
    }

    # Step 7: Write the final answer to the result CSV/log
    with open('output/result_log.json', 'w') as f:
        json.dump(final_answer, f)
    print(json.dumps(final_answer, ensure_ascii=False))