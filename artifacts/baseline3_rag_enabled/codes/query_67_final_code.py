import os
import pandas as pd
import pm4py
import json

def main():
    ocel = ACTIVE_LOG
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL for the object type 'employees'
    flattened_log = pm4py.ocel_flattening(ocel, 'employees')

    # Step 2: Count cases with exactly one event
    single_event_cases = flattened_log[flattened_log['case:concept:name'].duplicated(keep=False) == False]
    single_event_case_ids = single_event_cases['case:concept:name'].unique()

    # Step 3: Save the case IDs to a CSV file
    single_event_cases_df = pd.DataFrame(single_event_case_ids, columns=['case_id'])
    single_event_cases_df.to_csv(os.path.join(output_dir, 'single_event_cases_employees.csv'), index=False)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'single_event_cases_employees.csv')}')

    # Step 4: Prepare the final answer
    final_answer = {
        'single_event_case_ids': single_event_case_ids.tolist(),
        'count': len(single_event_case_ids)
    }

    # Step 5: Save the final answer as JSON
    with open(os.path.join(output_dir, 'final_answer.json'), 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'final_answer.json')}')

    print(json.dumps(final_answer, ensure_ascii=False))