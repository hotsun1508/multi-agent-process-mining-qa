import pm4py
import pandas as pd
import json
import os
import statistics


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)
    transition_durations = log_df.groupby(["concept:name", "case:concept:name"])["time:timestamp"].diff().dt.total_seconds().dropna()
    transition_durations = transition_durations.reset_index(drop=True)
    dfg_durations = {}

    for (src, tgt), count in dfg.items():
        durations = transition_durations[(log_df["concept:name"].shift() == src) & (log_df["concept:name"] == tgt)]
        if not durations.empty:
            avg_duration = durations.mean()
            dfg_durations[(src, tgt)] = avg_duration

    # Identify the edge with the highest average transition duration
    max_edge = max(dfg_durations.items(), key=lambda x: x[1])
    max_edge_info = {
        "source": max_edge[0][0],
        "target": max_edge[0][1],
        "average_duration": max_edge[1]
    }

    # Save DFG visualization
    png_path = "output/dfg_visualization.png"
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, png_path)
    print(f"OUTPUT_FILE_LOCATION: {png_path}")

    # Save DFG as a pickle file
    with open("output/dfg.pkl", "wb") as f:
        pickle.dump(dfg, f)
    print("OUTPUT_FILE_LOCATION: output/dfg.pkl")

    # Prepare final answer
    final_answer = {"max_edge": max_edge_info}
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()