import pm4py
import json
import os
import pickle


def main():
    event_log = ACTIVE_LOG
    heuristics_net = pm4py.discover_heuristics_net(event_log)
    png_path = 'output/heuristics_net_visualization.png'
    pm4py.save_vis_heuristics_net(heuristics_net, png_path)
    print(f'OUTPUT_FILE_LOCATION: {png_path}')
    with open('output/heuristics_net.pkl', 'wb') as f:
        pickle.dump(heuristics_net, f)
    print('OUTPUT_FILE_LOCATION: output/heuristics_net.pkl')
    total = sum(heuristics_net['dependencies'].values()) if heuristics_net['dependencies'] else 0
    top_edges = [
        {'source': src, 'target': dst, 'count': count, 'share': (count / total if total else 0.0)}
        for (src, dst), count in sorted(heuristics_net['dependencies'].items(), key=lambda x: x[1], reverse=True)[:10]
    ]
    final_answer = {'top_edges': top_edges}
    print(json.dumps(final_answer, ensure_ascii=False))