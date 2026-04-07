import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)
    heuristics_net = pm4py.discover_heuristics_net(dfg)
    png_path = "output/heuristics_net_visualization.png"
    pm4py.save_vis_heuristics_net(heuristics_net, png_path)
    print(f"OUTPUT_FILE_LOCATION: {png_path}")
    
    # Calculate transition durations
    transition_durations = []
    for case in log_df["case:concept:name"].unique():
        case_df = log_df[log_df["case:concept:name"] == case]
        case_df["time:timestamp"] = pd.to_datetime(case_df["time:timestamp"])
        case_df = case_df.sort_values("time:timestamp")
        durations = case_df["time:timestamp"].diff().dt.total_seconds().dropna()
        transition_durations.extend(durations)

    average_duration = sum(transition_durations) / len(transition_durations) if transition_durations else 0
    
    # Find the strongest dependency relation
    strongest_dependency = max(dfg.items(), key=lambda x: x[1]) if dfg else (None, 0)
    strongest_edge = strongest_dependency[0] if strongest_dependency[0] else (None, None)
    
    final_answer = {
        "strongest_dependency": strongest_edge,
        "average_transition_duration": average_duration
    }
    
    with open("output/benchmark_result.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/benchmark_result.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == "__main__":
    main()