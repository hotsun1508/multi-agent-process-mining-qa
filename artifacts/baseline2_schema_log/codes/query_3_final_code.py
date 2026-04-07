import pm4py
import json
import os
import pickle


def main():
    event_log = ACTIVE_LOG
    # Discover the Petri net using the Inductive Miner algorithm
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(event_log)
    
    # Save the Petri net model as a .pkl file
    pkl_path = 'output/petri_net.pkl'
    with open(pkl_path, 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)
    print(f'OUTPUT_FILE_LOCATION: {pkl_path}')  
    
    # Count places, transitions, and arcs
    num_places = len(petri_net.places)
    num_transitions = len(petri_net.transitions)
    num_arcs = len(petri_net.arcs)
    
    # Prepare the final answer
    final_answer = {
        'places': num_places,
        'transitions': num_transitions,
        'arcs': num_arcs
    }
    
    # Save the final answer to a JSON-serializable format
    with open('output/final_answer.json', 'w') as f:
        json.dump(final_answer, f)
    print(f'OUTPUT_FILE_LOCATION: output/final_answer.json')
    
    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()