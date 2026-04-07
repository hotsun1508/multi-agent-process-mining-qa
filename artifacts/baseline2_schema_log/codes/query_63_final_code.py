import pm4py
import json
import os
import pickle

def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for packages
    flat_log = pm4py.ocel_flattening(ocel, object_type='packages')
    # Discover the Petri net using the Inductive Miner
    ocpn = pm4py.discover_petri_net_inductive(flat_log)
    # Save the Petri net visualization
    png_path = 'output/im_packages.png'
    pm4py.save_vis_petri_net(ocpn, png_path)
    print(f'OUTPUT_FILE_LOCATION: {png_path}')  
    # Save the Petri net model
    pkl_path = 'output/im_packages.pkl'
    with open(pkl_path, 'wb') as f:
        pickle.dump(ocpn, f)
    print(f'OUTPUT_FILE_LOCATION: {pkl_path}')  
    # Count places, transitions, and arcs
    net = ocpn['petri_nets']['packages'][0]  # Access the Petri net for packages
    places_count = len(net.places)
    transitions_count = len(net.transitions)
    arcs_count = len(net.arcs)
    final_answer = {
        'places': places_count,
        'transitions': transitions_count,
        'arcs': arcs_count
    }
    print(json.dumps(final_answer, ensure_ascii=False))