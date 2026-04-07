import os
import json
import math
import pm4py
import pandas as pd


def run_top_variant_tbr_nonfitting_benchmark(event_log, top_pct=0.2, output_dir="output"):
    def prepare_case_dataframe(event_log):
        if event_log is None:
            raise ValueError("event_log is None. Expected a PM4Py EventLog.")
        if not hasattr(event_log, "__iter__"):
            raise TypeError("Invalid event_log type. Expected an iterable PM4Py EventLog.")
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
        from pm4py.algo.conformance.tokenbased import algorithm as tba
        replay_result = tba.apply(event_log, net, initial_marking)
        return replay_result

    df = prepare_case_dataframe(event_log)
    variant_freqs = get_variant_frequencies(event_log)
    top_n = math.ceil(len(variant_freqs) * top_pct)
    top_variants = [variant[0] for variant in variant_freqs[:top_n]]
    filtered_cases = df[df["case:concept:name"].isin(top_variants)]
    net, initial_marking, final_marking = discover_petri_net(filtered_cases)
    replay_result = token_based_replay(filtered_cases, net, initial_marking)

    non_fit_cases = [case for case in replay_result if not case['fit']]
    total_cases = len(non_fit_cases)
    avg_case_duration = (df.groupby("case:concept:name")["time:timestamp"].max() - df.groupby("case:concept:name")["time:timestamp"].min()).mean().total_seconds()

    delayed_cases = [case for case in non_fit_cases if case['duration'] > avg_case_duration]
    delayed_cases_count = len(delayed_cases)
    percentage_delayed = (delayed_cases_count / total_cases * 100) if total_cases > 0 else 0

    top_resources = filtered_cases[filtered_cases["case:concept:name"].isin([case['case_id'] for case in delayed_cases])]["org:resource"].value_counts().head(3).to_dict()

    final_answer = {
        "percentage_delayed": percentage_delayed,
        "top_resources": top_resources
    }

    with open(os.path.join(output_dir, "benchmark_results.json"), "w") as f:
        json.dump(final_answer, f, ensure_ascii=False)
    print(f"OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'benchmark_results.json')}")
    print(json.dumps(final_answer, ensure_ascii=False))


def main():
    event_log = ACTIVE_LOG
    run_top_variant_tbr_nonfitting_benchmark(event_log)