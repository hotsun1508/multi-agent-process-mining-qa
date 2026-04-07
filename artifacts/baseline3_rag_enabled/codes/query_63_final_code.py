import os
import json
import pm4py
import pickle

def main():
    ocel = ACTIVE_LOG
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL for the object type 'packages'
    flattened_log = pm4py.ocel_flattening(ocel, 'packages')

    # Step 2: Discover a Petri net using the Inductive Miner
    petri_net = pm4py.discover_petri_net_inductive(flattened_log)

    # Step 3: Save the Petri net model
    petri_net_pkl_path = os.path.join(output_dir, 'im_packages.pkl')
    with open(petri_net_pkl_path, 'wb') as f:
        pickle.dump(petri_net, f)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_pkl_path}')  

    # Step 4: Save the visualization of the Petri net
    petri_net_png_path = os.path.join(output_dir, 'im_packages.png')
    pm4py.save_vis_petri_net(petri_net, petri_net_png_path)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_png_path}')  

    # Step 5: Report the numbers of places, transitions, and arcs
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
    final_answer_path = os.path.join(output_dir, 'final_answer.json')
    with open(final_answer_path, 'w') as f:
        json.dump(final_answer, f)
    print(f'OUTPUT_FILE_LOCATION: {final_answer_path}')  

    # Return the final answer
    print(json.dumps(final_answer, ensure_ascii=False))