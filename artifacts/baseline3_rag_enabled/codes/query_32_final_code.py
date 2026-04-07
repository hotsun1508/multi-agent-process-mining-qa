import os
import json
import pm4py
import pandas as pd

def main():
    event_log = ACTIVE_LOG
    # Step 1: Convert the event log to a case dataframe
    df = pm4py.convert_to_dataframe(event_log)
    df["time:timestamp"] = pd.to_datetime(df["time:timestamp"])

    # Step 2: Get variant frequencies
    variant_counts = df.groupby(["case:concept:name", "concept:name"]).size().reset_index(name='counts')
    total_variants = variant_counts['counts'].sum()
    top_variants = variant_counts[variant_counts['counts'] >= (total_variants / 2)]
    top_variant_cases = df[df["case:concept:name"].isin(top_variants["case:concept:name"])]

    # Step 3: Discover the Petri net model using the Inductive Miner algorithm
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(top_variant_cases)
    petri_net_filename = "output/petri_net_model.pkl"
    pm4py.save_vis_petri_net(petri_net, petri_net_filename)
    print(f"OUTPUT_FILE_LOCATION: {petri_net_filename}")

    # Step 4: Run token-based replay to isolate non-fit cases
    non_fit_cases = pm4py.conformance_token_based_replay(top_variant_cases, petri_net, initial_marking, final_marking)
    non_fit_case_ids = non_fit_cases["case:concept:name"].unique()
    non_fit_cases_df = df[df["case:concept:name"].isin(non_fit_case_ids)]

    # Step 5: Get the top 5 resources appearing most frequently in non-fit cases
    top_resources = non_fit_cases_df["org:resource"].value_counts().head(5).to_dict()

    # Step 6: Prepare the final benchmark answer as a JSON-serializable dictionary
    answer = {
        "primary_answer_in_csv_log": True,
        "result_type": "single",
        "view": "event_log",
        "result_schema": {"process_discovery": "petri_net"},
        "artifacts_schema": ["output/* (optional auxiliary artifacts such as png/csv/pkl/json)"],
        "metrics": {
            "top_resources": top_resources
        }
    }

    # Step 7: Save the final answer to a JSON file
    answer_filename = "output/benchmark_answer.json"
    with open(answer_filename, "w", encoding="utf-8") as f:
        json.dump(answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: {answer_filename}")

    print(json.dumps(answer, ensure_ascii=False))