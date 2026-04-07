import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log, variant=pm4py.dfg.Variants.PERFORMANCE)
    # Calculate average transition durations
    transition_durations = {}
    for (src, tgt), count in dfg.items():
        durations = log_df[(log_df['concept:name'] == src) | (log_df['concept:name'] == tgt)]
        if not durations.empty:
            avg_duration = durations['time:timestamp'].diff().mean().total_seconds()
            transition_durations[(src, tgt)] = avg_duration
    # Identify the slowest edge
    slowest_edge = max(transition_durations, key=transition_durations.get)
    slowest_duration = transition_durations[slowest_edge]
    # Get resources involved in the source and target activities
    src_resources = log_df[log_df['concept:name'] == slowest_edge[0]]['org:resource'].value_counts().head(5).index.tolist()
    tgt_resources = log_df[log_df['concept:name'] == slowest_edge[1]]['org:resource'].value_counts().head(5).index.tolist()
    top_resources = list(set(src_resources + tgt_resources))[:5]
    # Save DFG visualization
    png_path = "output/dfg_performance_visualization.png"
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, png_path)
    print(f"OUTPUT_FILE_LOCATION: {png_path}")
    # Prepare final answer
    final_answer = {
        "slowest_edge": slowest_edge,
        "slowest_duration": slowest_duration,
        "top_resources": top_resources
    }
    with open("output/dfg_performance_analysis.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: output/dfg_performance_analysis.json")
    print(json.dumps(final_answer, ensure_ascii=False)


if __name__ == "__main__":
    main()