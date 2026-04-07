import pandas as pd
import json
import os


def main():
    ocel = ACTIVE_LOG
    
    # Step 1: Access the ocel.relations dataframe to gather event IDs and their corresponding object IDs
    relations_df = ocel.relations

    # Step 2: Group the data by 'ocel:eid' and count the unique 'ocel:oid' values for each event to determine the arity
    arity_df = relations_df.groupby('ocel:eid')['ocel:oid'].nunique().reset_index(name='arity')

    # Step 3: Sort the results in descending order based on the arity and select the top-10 events
    top_arity_events_df = arity_df.sort_values(by='arity', ascending=False).head(10)

    # Step 4: Save this DataFrame to a CSV file named 'top_arity_events.csv'
    top_arity_events_df.to_csv('output/top_arity_events.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/top_arity_events.csv')

    # Step 5: Construct the final JSON-serializable dictionary with the required structure
    result = {
        'primary_answer_in_csv_log': True,
        'result_type': 'single',
        'view': 'raw_ocel',
        'result_schema': {
            'object_interaction': 'table'
        },
        'artifacts_schema': ['output/* (optional auxiliary artifacts such as png/csv/pkl/json)']
    }

    # Step 6: Print the final answer
    print(json.dumps(result, ensure_ascii=False))