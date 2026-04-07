import pm4py
import pandas as pd
import json
import os


def main():
    ocel = ACTIVE_LOG
    # Access the relations from the OCEL
    relations = ocel.relations
    
    # Create a DataFrame from the relations
    df_relations = pd.DataFrame(relations)
    
    # Filter for the specific relation between orders and customers
    df_orders_customers = df_relations[(df_relations['ocel:type'] == 'orders') & (df_relations['ocel:qualifier'] == 'customers')]
    
    # Compute the multiplicity distribution
    multiplicity_distribution = df_orders_customers['ocel:oid'].value_counts().reset_index()
    multiplicity_distribution.columns = ['order_id', 'customer_count']
    
    # Save the multiplicity distribution to a CSV file
    multiplicity_csv_path = 'output/multiplicity_orders_customers.csv'
    multiplicity_distribution.to_csv(multiplicity_csv_path, index=False)
    print(f'OUTPUT_FILE_LOCATION: {multiplicity_csv_path}')  
    
    # Prepare the final answer
    final_answer = {'object_interaction': multiplicity_distribution.to_dict(orient='records')}
    
    # Save the final answer as a JSON file
    final_answer_json_path = 'output/final_answer.json'
    with open(final_answer_json_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {final_answer_json_path}')  
    
    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))