import pm4py
import pandas as pd
import json
import os


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for customers
    flattened_customers = pm4py.ocel_flattening(ocel, 'customers')
    
    # Create a DataFrame from the flattened view
    df = pd.DataFrame(flattened_customers)
    
    # Group by case and activity to get the trace variants
    variant_counts = df.groupby('case:concept:name')['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts().reset_index()
    variant_counts.columns = ['variant', 'frequency']
    
    # Save the variant frequency table to CSV
    variant_counts.to_csv('output/variants_customers.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/variants_customers.csv')
    
    # Prepare the final answer
    final_answer = {'unique_trace_variants': len(variant_counts)}
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()