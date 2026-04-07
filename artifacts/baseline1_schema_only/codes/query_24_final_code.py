import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Get the top-3 most frequent variants
    variant_counts = log_df.groupby("case:concept:name")["concept:name"].apply(lambda x: tuple(x)).value_counts()
    top_variants = variant_counts.head(3).index.tolist()
    
    # Filter the log for the top-3 variants
    filtered_log = log_df[log_df.groupby("case:concept:name")["concept:name"].transform(lambda x: tuple(x) in top_variants)]
    filtered_event_log = pm4py.convert_to_event_log(filtered_log)
    
    # Discover the Petri net using the Inductive Miner
    net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_event_log)
    
    # Count places, transitions, and arcs
    places_count = len(net.places)
    transitions_count = len(net.transitions)
    arcs_count = len(net.arcs)
    
    # Save the Petri net visualization
    png_path = "output/petri_net.png"
    pm4py.save_vis_petri_net(net, initial_marking, final_marking, png_path)
    print(f"OUTPUT_FILE_LOCATION: {png_path}")
    
    # Save the Petri net model
    with open("output/petri_net.pkl", "wb") as f:
        pickle.dump((net, initial_marking, final_marking), f)
    print("OUTPUT_FILE_LOCATION: output/petri_net.pkl")
    
    # Prepare the final answer
    final_answer = {
        "places": places_count,
        "transitions": transitions_count,
        "arcs": arcs_count
    }
    
    # Save the final answer as JSON
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))