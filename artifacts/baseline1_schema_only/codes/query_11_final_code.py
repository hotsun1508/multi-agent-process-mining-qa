def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    cases_with_complete_application = log_df[log_df['concept:name'] == 'W_Complete application']['case:concept:name'].unique()
    total_cases = log_df['case:concept:name'].unique()
    cases_without_complete_application = set(total_cases) - set(cases_with_complete_application)
    count_without_complete_application = len(cases_without_complete_application)
    final_answer = {'count_without_complete_application': count_without_complete_application}
    with open('output/result.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False)
    print('OUTPUT_FILE_LOCATION: output/result.json')
    print(json.dumps(final_answer, ensure_ascii=False))