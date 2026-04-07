import pm4py
import pandas as pd
import json
import os
import pickle


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)
    transition_durations = log_df.groupby(["concept:name", "case:concept:name"])['time:timestamp'].agg(["min", "max"]).reset_index()
    transition_durations["duration"] = (transition_durations["max"] - transition_durations["min"]).dt.total_seconds()
    transition_durations = transition_durations.groupby(["concept:name"])['duration'].mean().reset_index()
    slowest_edge = transition_durations.loc[transition_durations['duration'].idxmax()]
    slowest_activity = slowest_edge["concept:name"]
    slowest_duration = slowest_edge["duration"]

    # Find the cases involving the slowest edge
    involved_cases = log_df[log_df["concept:name"] == slowest_activity]["case:concept:name"].unique()
    involved_resources = log_df[log_df["case:concept:name"].isin(involved_cases)][["org:resource", "concept:name"]]
    top_resources = involved_resources["org:resource"].value_counts().head(5).index.tolist()

    # Find the dominant variant among the cases involving those resources
    filtered_cases = log_df[log_df["case:concept:name"].isin(involved_cases) & log_df["org:resource"].isin(top_resources)]
    variants = filtered_cases.groupby(["case:concept:name"])['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    dominant_variant = variants.idxmax() if not variants.empty else None

    # Save DFG visualization
    png_path = "output/dfg_visualization.png"
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, png_path)
    print(f"OUTPUT_FILE_LOCATION: {png_path}")

    # Save DFG as a pickle file
    with open("output/dfg.pkl", "wb") as f:
        pickle.dump(dfg, f)
    print("OUTPUT_FILE_LOCATION: output/dfg.pkl")

    # Prepare final answer
    final_answer = {
        "slowest_edge": {
            "activity": slowest_activity,
            "average_duration": slowest_duration,
            "top_resources": top_resources,
            "dominant_variant": dominant_variant
        }
    }
    print(json.dumps(final_answer, ensure_ascii=False))