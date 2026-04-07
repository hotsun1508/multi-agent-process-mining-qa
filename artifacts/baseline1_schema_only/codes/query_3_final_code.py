import pm4py
import json
import os
import pickle

def main():
    event_log = ACTIVE_LOG
    # Discover the Petri net using the Inductive Miner algorithm
    net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(event_log)
    # Save the Petri net model as a .pkl file
    model_path = 'output/petri_net_model.pkl'
    with open(model_path, 'wb') as f:
        pickle.dump((net, initial_marking, final_marking), f)
    print(f'OUTPUT_FILE_LOCATION: {model_path}')  
    # Count places, transitions, and arcs
    places_count = len(net.places)
    transitions_count = len(net.transitions)
    arcs_count = len(net.arcs)
    # Prepare the final answer
    final_answer = {
        'places': places_count,
        'transitions': transitions_count,
        'arcs': arcs_count
    }
    print(json.dumps(final_answer, ensure_ascii=False))