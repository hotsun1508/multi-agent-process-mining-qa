import pm4py
import pandas as pd
import json
import os


def analyze_event_log(event_log):
    os.makedirs("output", exist_ok=True)

    # Step 1: Convert the event log from XES format to a DataFrame
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])

    # Step 2: Identify the strongest working together pair of resources
    resource_counts = log_df.groupby('org:resource')['case:concept:name'].nunique().reset_index()
    resource_counts.columns = ['resource', 'case_count']
    resource_pairs = []

    for i in range(len(resource_counts)):
        for j in range(i + 1, len(resource_counts)):
            resource_1 = resource_counts.iloc[i]['resource']
            resource_2 = resource_counts.iloc[j]['resource']
            pair_count = log_df[(log_df['org:resource'] == resource_1) | (log_df['org:resource'] == resource_2)]
            pair_count = pair_count['case:concept:name'].nunique()
            resource_pairs.append((resource_1, resource_2, pair_count))

    strongest_pair = max(resource_pairs, key=lambda x: x[2])
    resource_1, resource_2 = strongest_pair[0], strongest_pair[1]

    # Step 3: Filter the DataFrame to include only cases involving the strongest pair
    filtered_log_df = log_df[(log_df['org:resource'] == resource_1) | (log_df['org:resource'] == resource_2)]
    filtered_cases = filtered_log_df['case:concept:name'].unique()

    # Step 4: Determine the dominant variant in the filtered cases
    variant_counts = filtered_log_df.groupby('case:concept:name')['concept:name'].apply(lambda x: tuple(x)).value_counts()
    dominant_variant = variant_counts.idxmax()

    # Step 5: Keep only delayed cases in the dominant variant whose total case duration exceeds the overall average case duration
    case_durations = filtered_log_df.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
    case_durations['duration'] = (case_durations[('time:timestamp', 'max')] - case_durations[('time:timestamp', 'min')]).dt.total_seconds()
    overall_avg_duration = case_durations['duration'].mean()
    delayed_cases = case_durations[case_durations['duration'] > overall_avg_duration].index.tolist()
    delayed_cases_df = filtered_log_df[filtered_log_df['case:concept:name'].isin(delayed_cases)]

    # Step 6: Discover a Petri net from those delayed cases
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(delayed_cases_df)
    pm4py.save_vis_petri_net(petri_net, "output/petri_net.png")
    print("OUTPUT_FILE_LOCATION: output/petri_net.png")

    # Step 7: Perform token-based replay conformance checking
    token_based_replay_results = pm4py.conformance_token_based_replay(delayed_cases_df, petri_net, initial_marking)
    fit_cases_count = sum(1 for result in token_based_replay_results if result['fit'])

    # Step 8: Prepare final answer
    final_answer = {
        "resource": {"resource_1": resource_1, "resource_2": resource_2},
        "behavior_variant": dominant_variant,
        "performance": {
            "overall_avg_duration": overall_avg_duration,
            "delayed_cases_count": len(delayed_cases),
            "fit_cases_count": fit_cases_count
        },
        "process_discovery": {
            "petri_net": "output/petri_net.png"
        },
        "conformance": {
            "fit_cases_count": fit_cases_count
        }
    }

    print(json.dumps(final_answer, ensure_ascii=False))


def main():
    event_log = ACTIVE_LOG
    analyze_event_log(event_log)