import pm4py
import pandas as pd
import json
import os


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for customers
    flattened_customers = pm4py.ocel_flattening(ocel, object_type='customers')
    # Create a DataFrame from the flattened log
    df = pd.DataFrame(flattened_customers)
    # Group by case and count unique trace variants
    variant_counts = df.groupby('case:concept:name')['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    # Save the variant frequency table to CSV
    variant_counts_df = variant_counts.reset_index(name='frequency')
    variant_counts_df.columns = ['variant', 'frequency']
    variant_counts_df.to_csv('output/variants_customers.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/variants_customers.csv')
    # Prepare the final answer
    final_answer = {'unique_trace_variants': len(variant_counts)}
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()