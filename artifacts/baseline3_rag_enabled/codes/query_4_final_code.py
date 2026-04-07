import pm4py
import pandas as pd
import json
import os
import pickle

def main():
    event_log = ACTIVE_LOG
    os.makedirs("output", exist_ok=True)

    # Convert event log to DataFrame
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])

    # Discover the heuristics net
    heuristics_net = pm4py.discover_heuristics_net(event_log)

    # Save the heuristics net model as a .pkl file
    heuristics_net_path = "output/heuristics_net_model.pkl"
    with open(heuristics_net_path, 'wb') as f:
        pickle.dump(heuristics_net, f)
    print(f"OUTPUT_FILE_LOCATION: {heuristics_net_path}")

    # Visualize the heuristics net and save as PNG
    vis_png_path = "output/heuristics_net_visualization.png"
    pm4py.save_vis_heuristics_net(heuristics_net, vis_png_path)
    print(f"OUTPUT_FILE_LOCATION: {vis_png_path}")

    # Extract dependencies
    dependencies = []
    if heuristics_net is not None and hasattr(heuristics_net, "dependency_matrix"):
        for source, targets in heuristics_net.dependency_matrix.items():
            for target, weight in targets.items():
                dependencies.append((source, target, weight))

    # Get top-10 strongest dependencies
    top_10_dependencies = sorted(dependencies, key=lambda x: x[2], reverse=True)[:10]

    # Construct the final JSON-serializable dictionary
    final_answer = {
        "top_dependencies": top_10_dependencies
    }
    with open("output/result.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(json.dumps(final_answer, ensure_ascii=False))