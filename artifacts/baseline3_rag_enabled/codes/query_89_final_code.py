import pm4py
import json
import os
import pickle

def main():
    ocel = ACTIVE_LOG
    # Step 1: Discover the Object-Centric Petri Net (OCPN)
    ocpn = pm4py.discover_oc_petri_net(ocel)
    
    # Step 2: Analyze transitions to find the one with the maximum number of linked objects
    transition_linked_objects = {}
    for object_type, (net, initial_marking, final_marking) in ocpn['petri_nets'].items():
        for transition in net.transitions:
            linked_objects = set()
            for arc in net.arcs:
                if arc.source == transition:
                    linked_objects.update(arc.target)
            transition_linked_objects[transition.label] = len(linked_objects)
    
    # Find the transition with the maximum count of linked objects
    max_transition = max(transition_linked_objects, key=transition_linked_objects.get)
    max_count = transition_linked_objects[max_transition]
    
    # Prepare statistics
    statistics = {
        'places': len(net.places),
        'transitions': len(net.transitions),
        'arcs': len(net.arcs),
        'object_types': len(ocpn['petri_nets']),
    }
    
    # Save the OCPN model
    ocpn_pkl_path = 'output/ocpn.pkl'
    with open(ocpn_pkl_path, 'wb') as f:
        pickle.dump(ocpn, f)
    print(f'OUTPUT_FILE_LOCATION: {ocpn_pkl_path}')  
    
    # Prepare the final answer
    final_answer = {
        'max_transition': max_transition,
        'max_count': max_count,
        'statistics': statistics,
    }
    
    # Save the final answer to a JSON file
    final_answer_path = 'output/final_answer.json'
    with open(final_answer_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {final_answer_path}')  
    
    print(json.dumps(final_answer, ensure_ascii=False))