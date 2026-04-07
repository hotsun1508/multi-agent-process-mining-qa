def main():
    event_log = ACTIVE_LOG
    # Step 1: Convert the event log to a case dataframe
    df = pm4py.convert_to_dataframe(event_log)
    # Step 2: Compute the co-occurrence of resources for each case
    co_occurrence_matrix = df.groupby('case:concept:name')['org:resource'].apply(lambda x: x.value_counts()).unstack(fill_value=0)
    # Step 3: Save the co-occurrence matrix as a CSV file
    co_occurrence_matrix.to_csv('output/co_occurrence_matrix.csv')
    print('OUTPUT_FILE_LOCATION: output/co_occurrence_matrix.csv')
    # Step 4: Prepare the result in the required JSON format
    result = {
        'primary_answer_in_csv_log': True,
        'result_type': 'single',
        'view': 'event_log',
        'result_schema': {'resource': 'matrix'},
        'artifacts_schema': ['output/* (optional auxiliary artifacts such as png/csv/pkl/json)'],
        'result': co_occurrence_matrix.to_dict()
    }
    print(json.dumps(result, ensure_ascii=False))