import pm4py
import pandas as pd
import os
import pickle


def main():
    event_log = ACTIVE_LOG
    os.makedirs("output", exist_ok=True)

    # Discover the Directly-Follows Graph (DFG)
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)

    # Save DFG visualization as PNG
    png_path = "output/dfg_visualization.png"
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, png_path)
    print(f"OUTPUT_FILE_LOCATION: {png_path}")

    # Save DFG object as .pkl file
    with open("output/dfg.pkl", "wb") as f:
        pickle.dump(dfg, f)
    print("OUTPUT_FILE_LOCATION: output/dfg.pkl")

    # Calculate top-10 edges by frequency
    total = sum(dfg.values()) if dfg else 0
    top_edges = [
        {"source": src, "target": dst, "count": count, "share": (count / total if total else 0.0)}
        for (src, dst), count in sorted(dfg.items(), key=lambda x: x[1], reverse=True)[:10]
    ]

    # Prepare final answer
    final_answer = {"top_edges": top_edges}
    print(json.dumps(final_answer, ensure_ascii=False))