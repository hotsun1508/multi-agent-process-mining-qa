import pm4py
import json


def main():
    ocel = ACTIVE_LOG
    ocpn = pm4py.discover_oc_petri_net(ocel)
    png_path = 'output/ocpn.png'
    pm4py.save_vis_ocpn(ocpn, png_path)
    print(f'OUTPUT_FILE_LOCATION: {png_path}')
    transition_linked_objects = {}
    for object_type, (net, initial_marking, final_marking) in ocpn['petri_nets'].items():
        for transition in net.transitions:
            linked_objects = set()
            for arc in net.arcs:
                if arc.source == transition:
                    linked_objects.update(arc.target)
            transition_linked_objects[transition.label] = len(linked_objects)
    max_transition = max(transition_linked_objects, key=transition_linked_objects.get)
    max_count = transition_linked_objects[max_transition]
    final_answer = {'max_transition': max_transition, 'linked_objects_count': max_count}
    with open('output/ocpn_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/ocpn_analysis.json')
    print(json.dumps(final_answer, ensure_ascii=False))