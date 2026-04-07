import pm4py
import pandas as pd
import json
import os

def compute_strongest_handover_in_most_freq_variant(event_log, output_dir="output"):
    # Convert the event log to a case dataframe
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])

    # Step 1: Identify the most frequent variant
    variant_counts = log_df.groupby(['case:concept:name'])['concept:name'].apply(lambda x: ' > '.join(x)).value_counts()
    most_frequent_variant = variant_counts.idxmax()

    # Step 2: Filter the log for the most frequent variant
    filtered_log_df = log_df[log_df.groupby(['case:concept:name'])['concept:name'].apply(lambda x: ' > '.join(x)) == most_frequent_variant]

    # Step 3: Compute the co-occurrence of resources for each case
    co_occurrence = pd.DataFrame(0, index=filtered_log_df['org:resource'].unique(), columns=filtered_log_df['org:resource'].unique())
    for case_id, group in filtered_log_df.groupby('case:concept:name'):
        resources = group['org:resource'].dropna().unique()
        for i in range(len(resources)):
            for j in range(i + 1, len(resources)):
                co_occurrence.loc[resources[i], resources[j]] += 1
                co_occurrence.loc[resources[j], resources[i]] += 1

    # Step 4: Identify the strongest handover of work relation
    strongest_pair = co_occurrence.stack().idxmax()
    strongest_value = co_occurrence.stack().max()

    # Step 5: Prepare the final output dictionary
    final_answer = {
        "most_frequent_variant": most_frequent_variant,
        "strongest_handover_pair": strongest_pair,
        "handover_count": strongest_value
    }

    # Step 6: Save the co-occurrence matrix as a CSV file
    co_occurrence.to_csv(os.path.join(output_dir, 'co_occurrence_matrix.csv'))
    print(f"OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'co_occurrence_matrix.csv')}")

    # Step 7: Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))

def main():
    event_log = ACTIVE_LOG
    compute_strongest_handover_in_most_freq_variant(event_log)