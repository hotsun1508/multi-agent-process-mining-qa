import pm4py
import pandas as pd
import json


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    # Filter cases that do not contain 'W_Complete application'
    cases_with_complete_application = log_df[log_df['concept:name'] == 'W_Complete application']['case:concept:name'].unique()
    total_cases = log_df['case:concept:name'].unique()
    cases_without_complete_application = set(total_cases) - set(cases_with_complete_application)
    count_cases_without_complete_application = len(cases_without_complete_application)
    
    # Prepare final answer
    final_answer = {'count_cases_without_complete_application': count_cases_without_complete_application}
    
    # Save the final answer to a JSON file
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    
    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))