import os
import json
import numpy as np
import pm4py
from pm4py.objects.log.util import get_case_durations
from pm4py.algo.discovery import petri_net as pn_discovery
from pm4py.algo.conformance import token_based_replay


def run_top_variant_tbr_nonfitting_benchmark(event_log, top_pct=0.2, output_dir="output"):
    def prepare_case_dataframe(event_log):
        df_local = pm4py.convert_to_dataframe(event_log)
        required_cols = ["case:concept:name", "concept:name", "time:timestamp", "org:resource"]
        df_local["time:timestamp"] = pd.to_datetime(df_local["time:timestamp"], utc=True, errors="coerce")
        return df_local

    def get_variant_frequencies(event_log):
        variants = pm4py.get_variants(event_log)
        items = []
        for k, v in variants.items():
            cnt = v if isinstance(v, int) else len(v)
            items.append((k, int(cnt)))
        return sorted(items, key=lambda x: x[1], reverse=True)

    def filter_top_variants(df, top_pct):
        variant_counts = df["concept:name"].value_counts()
        top_n = int(len(variant_counts) * top_pct)
        top_variants = variant_counts.head(top_n).index.tolist()
        return df[df["concept:name"].isin(top_variants)]

    df = prepare_case_dataframe(event_log)
    case_durations = get_case_durations(event_log)
    average_duration = np.mean(case_durations)
    df["case_duration"] = df.groupby("case:concept:name")[
        "time:timestamp"].transform(lambda x: (x.max() - x.min()).total_seconds())
    ]
    delayed_cases = df[df["case_duration"] > average_duration]
    top_variant_cases = filter_top_variants(delayed_cases, top_pct)

    # Discover Petri net from delayed cases
    petri_net, initial_marking, final_marking = pn_discovery.apply(top_variant_cases)
    pn_path = os.path.join(output_dir, "petri_net.png")
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, pn_path)
    print(f"OUTPUT_FILE_LOCATION: {pn_path}")

    # Token-based replay to find non-fit cases
    replay_results = token_based_replay.apply(top_variant_cases, petri_net, initial_marking)
    non_fit_cases = [case for case, result in replay_results.items() if result['fit'] == False]

    # Identify top 3 resources in non-fit cases
    non_fit_resources = top_variant_cases[top_variant_cases["case:concept:name"].isin(non_fit_cases)]["org:resource"].value_counts().head(3)
    top_resources = non_fit_resources.index.tolist()

    final_answer = {
        "top_resources": top_resources,
        "average_case_duration": average_duration,
        "total_delayed_cases": len(delayed_cases),
        "total_top_variant_cases": len(top_variant_cases),
        "total_non_fit_cases": len(non_fit_cases)
    }
    with open(os.path.join(output_dir, "benchmark_result.json"), "w") as f:
        json.dump(final_answer, f, ensure_ascii=False)
    print(f"OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'benchmark_result.json')}")
    print(json.dumps(final_answer, ensure_ascii=False))


def main():
    event_log = ACTIVE_LOG
    run_top_variant_tbr_nonfitting_benchmark(event_log)