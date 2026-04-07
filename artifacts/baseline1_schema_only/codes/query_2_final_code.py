import pm4py
import json
import pickle


def main():
    event_log = ACTIVE_LOG
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)
    png_path = 'output/dfg_visualization.png'
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, png_path)
    print(f'OUTPUT_FILE_LOCATION: {png_path}')
    with open('output/dfg.pkl', 'wb') as f:
        pickle.dump(dfg, f)
    print('OUTPUT_FILE_LOCATION: output/dfg.pkl')
    total = sum(dfg.values()) if dfg else 0
    top_edges = [
        {'source': src, 'target': dst, 'count': count, 'share': (count / total if total else 0.0)}
        for (src, dst), count in sorted(dfg.items(), key=lambda x: x[1], reverse=True)[:10]
    ]
    final_answer = {'top_edges': top_edges}
    print(json.dumps(final_answer, ensure_ascii=False))