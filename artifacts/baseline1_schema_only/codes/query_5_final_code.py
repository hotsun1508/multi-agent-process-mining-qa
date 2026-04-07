def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log)
    unique_variants = log_df.groupby('case:concept:name')['concept:name'].apply(lambda x: ' -> '.join(x)).unique()
    total_unique_variants = len(unique_variants)
    final_answer = {'total_unique_trace_variants': total_unique_variants}
    with open('output/unique_trace_variants.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/unique_trace_variants.json')
    print(json.dumps(final_answer, ensure_ascii=False))