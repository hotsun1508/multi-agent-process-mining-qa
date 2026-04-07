def main():
    event_log = ACTIVE_LOG
    import pandas as pd
    # Step 1: Convert the event log to a case dataframe
    df = pm4py.convert_to_dataframe(event_log)
    # Step 2: Count the frequency of events per resource
    resource_counts = df['org:resource'].value_counts().head(5)
    # Step 3: Prepare the result in the required JSON-serializable format
    result = {
        'resource': resource_counts.index.tolist(),
        'frequency': resource_counts.values.tolist()
    }
    # Step 4: Save the primary answer to the result CSV/log
    result_df = pd.DataFrame(result)
    result_df.to_csv('output/top_resources.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/top_resources.csv')
    # Step 5: Prepare the final output dictionary
    final_answer = {
        'status': 'success',
        'result_type': 'single',
        'view': 'event_log',
        'result': result
    }
    print(json.dumps(final_answer, ensure_ascii=False))