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
    orders_objects = ocel.objects[ocel.objects['ocel:type'] == 'orders']['ocel:oid']
    items_objects = ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid']
    filtered_relations = ocel.relations[(ocel.relations['ocel:oid'].isin(orders_objects)) | (ocel.relations['ocel:oid'].isin(items_objects))]
    filtered_events = ocel.events[ocel.events['ocel:eid'].isin(filtered_relations['ocel:eid'])]
    filtered_ocel = pm4py.objects.ocel.obj.OCEL(events=filtered_events, objects=ocel.objects, relations=filtered_relations)

    # Step 2: Discover OC-DFG
    ocdfg = pm4py.discover_ocdfg(filtered_ocel)
    ocdfg_path = 'output/ocdfg.png'
    pm4py.save_vis_ocdfg(ocdfg, ocdfg_path, annotation='frequency')
    print(f'OUTPUT_FILE_LOCATION: {ocdfg_path}')  

    # Step 3: Flatten the OCEL for the object type 'packages'
    flattened_packages = pm4py.ocel_flattening(filtered_ocel, object_type='packages')
    flattened_df = pd.DataFrame(flattened_packages)

    # Step 4: Calculate unique variants
    unique_variants = flattened_df['concept:name'].nunique()

    # Step 5: Calculate average case duration
    case_durations = flattened_df.groupby('case:concept:name')['time:timestamp'].max() - flattened_df.groupby('case:concept:name')['time:timestamp'].min()
    average_case_duration = case_durations.mean()

    # Step 6: Calculate share of cases exceeding average duration
    exceeding_cases = (case_durations > average_case_duration).mean()

    # Step 7: Prepare final answer
    final_answer = {
        'object_interaction': {'ocdfg': {'total_nodes': len(ocdfg['activities']), 'total_edges': sum(len(edge_map) for edge_map in ocdfg['edges']['event_couples'].values())}},
        'process_discovery': {'ocdfg': ocdfg},
        'behavior_variant': {'unique_variants': unique_variants},
        'performance': {'exceeding_cases_ratio': exceeding_cases}
    }

    # Save final answer
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')

    print(json.dumps(final_answer, ensure_ascii=False))