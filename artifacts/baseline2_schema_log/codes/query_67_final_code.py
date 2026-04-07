import pm4py
import pandas as pd
import json
import os

def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for employees
    flattened_employees = pm4py.ocel_flattening(ocel, object_type='employees')
    # Convert to DataFrame for easier manipulation
    df = pm4py.convert_to_dataframe(flattened_employees)
    # Count the number of events per case
    case_counts = df['case:concept:name'].value_counts()
    # Filter cases with exactly one event
    single_event_cases = case_counts[case_counts == 1].index.tolist()
    # Save the case IDs to a CSV file
    single_event_cases_df = pd.DataFrame(single_event_cases, columns=['case_id'])
    single_event_cases_df.to_csv('output/single_event_cases_employees.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/single_event_cases_employees.csv')
    # Prepare the final answer
    final_answer = {'single_event_case_count': len(single_event_cases)}
    print(json.dumps(final_answer, ensure_ascii=False))