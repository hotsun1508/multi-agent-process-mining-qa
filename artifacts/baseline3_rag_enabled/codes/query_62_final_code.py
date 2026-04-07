import pm4py
import json
import os
import pickle

def main():
    ocel = ACTIVE_LOG
    ocpn = pm4py.discover_oc_petri_net(ocel)
    png_path = 'output/ocpn.png'
    pm4py.save_vis_ocpn(ocpn, png_path)
    print(f'OUTPUT_FILE_LOCATION: {png_path}')
    with open('output/ocpn.pkl', 'wb') as f:
        pickle.dump(ocpn, f)
    print('OUTPUT_FILE_LOCATION: output/ocpn.pkl')
    per_object_type = {}
    for object_type, (net, initial_marking, final_marking) in ocpn['petri_nets'].items():
        per_object_type[object_type] = {
            'places': len(net.places),
            'transitions': len(net.transitions),
            'arcs': len(net.arcs),
        }
    final_answer = {'graph_type': 'ocpn', 'per_object_type': per_object_type}
    print(json.dumps(final_answer, ensure_ascii=False))