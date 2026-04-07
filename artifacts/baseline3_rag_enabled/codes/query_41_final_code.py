import os
import json
import pm4py
import pandas as pd


def run_top_variant_tbr_nonfitting_benchmark(event_log, top_pct=0.2, output_dir="output"):
    def prepare_case_dataframe(event_log):
        df_local = pm4py.convert_to_dataframe(event_log)
        required_cols = ["case:concept:name", "concept:name", "time:timestamp", "org:resource"]
        missing = [c for c in required_cols if c not in df_local.columns]
        if missing:
            raise ValueError(f"Missing required columns after conversion: {missing}")
        df_local["time:timestamp"] = pd.to_datetime(df_local["time:timestamp"], utc=True, errors="coerce")
        return df_local

    def get_variant_frequencies(event_log):
        variants = pm4py.get_variants(event_log)
        items = []
        for k, v in variants.items():
            cnt = v if isinstance(v, int) else len(v)
            items.append((k, int(cnt)))
        return sorted(items, key=lambda x: x[1], reverse=True)

    def discover_petri_net(event_log):
        net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(event_log)
        return net, initial_marking, final_marking

    def token_based_replay(event_log, net, initial_marking):
        from pm4py.algo.conformance.token_based_replay import algorithm as tbr
        return tbr.apply(event_log, net, initial_marking)

    df = prepare_case_dataframe(event_log)
    variant_frequencies = get_variant_frequencies(event_log)
    top_n = int(len(variant_frequencies) * top_pct)
    top_variants = [variant[0] for variant in variant_frequencies[:top_n]]
    filtered_cases = df[df["case:concept:name"].isin(top_variants)]

    # Discover Petri net from top variants
    petri_net, initial_marking, final_marking = discover_petri_net(filtered_cases)
    pm4py.save_vis_petri_net(petri_net, "output/petri_net.png")
    print("OUTPUT_FILE_LOCATION: output/petri_net.png")

    # Token-based replay
    non_fit_cases = []
    for case in filtered_cases["case:concept:name"].unique():
        case_log = filtered_cases[filtered_cases["case:concept:name"] == case]
        if not token_based_replay(case_log, petri_net, initial_marking):
            non_fit_cases.append(case)

    non_fit_df = filtered_cases[filtered_cases["case:concept:name"].isin(non_fit_cases)]
    top_resources = non_fit_df["org:resource"].value_counts().head(5).to_dict()

    final_answer = {
        "top_resources": top_resources,
        "petri_net": "output/petri_net.png"
    }
    with open("output/benchmark_result.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/benchmark_result.json")
    print(json.dumps(final_answer, ensure_ascii=False)


def main():
    event_log = ACTIVE_LOG
    run_top_variant_tbr_nonfitting_benchmark(event_log)