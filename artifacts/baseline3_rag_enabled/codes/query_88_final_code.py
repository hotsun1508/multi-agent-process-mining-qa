import os
import pandas as pd
import pm4py
import json

def main():
    ocel = ACTIVE_LOG
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Filter events linked to 'orders' and 'items'
    order_objects = ocel.objects[ocel.objects['ocel:type'] == 'orders']['ocel:oid']
    item_objects = ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid']

    order_relations = ocel.relations[ocel.relations['ocel:oid'].isin(order_objects)]
    item_relations = ocel.relations[ocel.relations['ocel:oid'].isin(item_objects)]

    # Find events linked to both orders and items
    linked_events = set(order_relations['ocel:eid']).intersection(set(item_relations['ocel:eid']))
    filtered_events = ocel.events[ocel.events['ocel:eid'].isin(linked_events)]

    # Create a restricted OCEL
    restricted_ocel = pm4py.objects.ocel.obj.OCEL(events=filtered_events, objects=ocel.objects, relations=ocel.relations)

    # Step 2: Flatten the restricted OCEL using 'orders' as the case notion
    flattened_log = pm4py.ocel_flattening(restricted_ocel, object_type='orders')

    # Step 3: Count unique variants
    unique_variants = flattened_log['concept:name'].nunique()

    # Save the unique variants to a CSV file
    variants_df = pd.DataFrame(flattened_log['concept:name'].value_counts()).reset_index()
    variants_df.columns = ['variant', 'frequency']
    variants_df.to_csv(os.path.join(output_dir, 'variants_orders_items_orders.csv'), index=False)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'variants_orders_items_orders.csv')}')

    # Prepare the final answer
    final_answer = {
        'unique_variants_count': unique_variants,
        'output_file': 'variants_orders_items_orders.csv'
    }
    print(json.dumps(final_answer, ensure_ascii=False))