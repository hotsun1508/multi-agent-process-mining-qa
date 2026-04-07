import os
import json
import pandas as pd
import pm4py
from pm4py.algo.conformance.token_based_replay import algorithm as tbr
from pm4py.objects.petri import petri_net as pn


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Get variant frequencies
    variants = pm4py.get_variants(event_log)
    variant_counts = sorted(variants.items(), key=lambda x: x[1], reverse=True)
    top_20_pct_index = int(len(variant_counts) * 0.2)
    top_variants = [v[0] for v in variant_counts[:top_20_pct_index]]
    
    # Filter log for top variants
    filtered_log = pm4py.filtering.log.filter_variants(event_log, top_variants)
    
    # Load Petri net
    petri_net, initial_marking, final_marking = pn.import_petri_net("/path/to/petri_net.pn")
    
    # Token-based replay
    replay_result = tbr.apply(filtered_log, petri_net, initial_marking, final_marking)
    non_fit_cases = [case for case in replay_result if not case['fit']]
    
    # Calculate sojourn times for non-fit cases
    non_fit_df = pm4py.convert_to_dataframe(non_fit_cases)
    non_fit_df["time:timestamp"] = pd.to_datetime(non_fit_df["time:timestamp"])
    non_fit_df["duration"] = non_fit_df.groupby("case:concept:name")["time:timestamp"].transform(lambda x: x.max() - x.min()).dt.total_seconds()
    
    # Find activity with longest average sojourn time
    avg_sojourn = non_fit_df.groupby("concept:name")["duration"].mean().reset_index()
    longest_activity = avg_sojourn.loc[avg_sojourn["duration"].idxmax()]
    longest_activity_name = longest_activity["concept:name"]
    
    # Get top 3 resources for that activity
    top_resources = non_fit_df[non_fit_df["concept:name"] == longest_activity_name]["org:resource"].value_counts().head(3)
    
    # Prepare final answer
    final_answer = {
        "longest_activity": longest_activity_name,
        "top_resources": top_resources.index.tolist(),
        "top_resources_counts": top_resources.values.tolist()
    }
    
    # Save final answer to JSON
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))