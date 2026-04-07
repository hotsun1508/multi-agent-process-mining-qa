import os
import pandas as pd
import json


def main():
    ocel = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Ensure required columns exist in objects and relations tables
    objects_df = ocel.objects
    relations_df = ocel.relations

    # Filter relations for orders and customers
    orders_customers_relations = relations_df[(relations_df['ocel:oid'].isin(objects_df[objects_df['ocel:type'] == 'orders']['ocel:oid'])) & (relations_df['ocel:oid'].isin(objects_df[objects_df['ocel:type'] == 'customers']['ocel:oid']))]

    # Count the number of customers per order
    multiplicity_distribution = orders_customers_relations['ocel:oid'].value_counts().reset_index()
    multiplicity_distribution.columns = ['order_oid', 'customer_count']

    # Save the multiplicity distribution to CSV
    multiplicity_csv_path = os.path.join(output_dir, 'multiplicity_orders_customers.csv')
    multiplicity_distribution.to_csv(multiplicity_csv_path, index=False)
    print(f'OUTPUT_FILE_LOCATION: {multiplicity_csv_path}')  

    # Prepare the final answer
    final_answer = {
        'object_interaction': multiplicity_distribution.to_dict(orient='records'),
    }

    # Save the final answer as JSON
    final_answer_json_path = os.path.join(output_dir, 'final_answer.json')
    with open(final_answer_json_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {final_answer_json_path}')  

    print(json.dumps(final_answer, ensure_ascii=False))