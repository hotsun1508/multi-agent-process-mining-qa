import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    variant_counts = log_df.groupby("case:concept:name")["concept:name"].apply(lambda x: " -> ".join(x)).value_counts()
    top_50_percent_variants = variant_counts.head(len(variant_counts) // 2).index.tolist()
    filtered_cases = log_df[log_df.groupby("case:concept:name")["concept:name"].transform(lambda x: " -> ".join(x)).isin(top_50_percent_variants)]
    filtered_event_log = pm4py.convert_to_event_log(filtered_cases)
    
    # Discover Petri net
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_event_log)
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, "output/petri_net.png")
    print(f"OUTPUT_FILE_LOCATION: output/petri_net.png")
    
    # Token-based replay
    fitness = pm4py.fitness_token_based_replay(filtered_event_log, petri_net, initial_marking, final_marking)
    fit_cases_count = sum(1 for case in fitness if fitness[case][0] == 1.0)
    
    final_answer = {
        "petri_net": {
            "places": len(petri_net.places),
            "transitions": len(petri_net.transitions),
            "fit_cases_count": fit_cases_count
        }
    }
    
    with open("output/benchmark_result.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: output/benchmark_result.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))