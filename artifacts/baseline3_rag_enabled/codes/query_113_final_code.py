import os
import pandas as pd
import pm4py
import json
import statistics

def main():
    ocel = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Filter events linked to orders and customers
    orders_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'orders']['ocel:oid'])
    customers_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid'])
    filtered_events = [event for event in ocel.events if any(rel['ocel:oid'] in orders_objects for rel in ocel.relations[event['ocel:eid']]) and any(rel['ocel:oid'] in customers_objects for rel in ocel.relations[event['ocel:eid']])]
    filtered_ocel = pm4py.objects.ocel.obj.OCEL(events=filtered_events, objects=ocel.objects, relations=ocel.relations)

    # Step 2: Flatten the restricted OCEL using customers as the case notion
    flattened_ocel = pm4py.ocel_flattening(filtered_ocel, object_type='customers')
    log_df = pm4py.convert_to_dataframe(flattened_ocel)

    # Step 3: Select the top 20% variants by frequency
    variant_counts = log_df['concept:name'].value_counts()
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()
    top_variant_cases = log_df[log_df['concept:name'].isin(top_variants)]

    # Step 4: Calculate the average case duration
    case_durations = top_variant_cases.groupby('case:concept:name')['time:timestamp'].max() - top_variant_cases.groupby('case:concept:name')['time:timestamp'].min()
    average_duration = case_durations.mean()

    # Step 5: Calculate the share of cases exceeding the average duration
    exceeding_cases = case_durations[case_durations > average_duration].count()
    total_cases = case_durations.count()
    exceeding_cases_ratio = exceeding_cases / total_cases if total_cases > 0 else 0.0

    # Step 6: Discover a Petri net from the top-variant subset
    top_variant_log = pm4py.convert_to_dataframe(top_variant_cases)
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(top_variant_log)

    # Step 7: Compute token-based replay fitness on the full restricted flattened view
    fitness = pm4py.fitness_token_based_replay(petri_net, initial_marking, final_marking, log_df)

    # Step 8: Save outputs
    pm4py.save_vis_petri_net(petri_net, os.path.join(output_dir, 'petri_net.png'))
    with open(os.path.join(output_dir, 'fitness.json'), 'w') as f:
        json.dump(fitness, f)

    # Final answer
    final_answer = {
        'exceeding_cases_ratio': exceeding_cases_ratio,
        'petri_net': 'petri_net.png',
        'top_variants': top_variants
    }
    with open(os.path.join(output_dir, 'final_answer.json'), 'w') as f:
        json.dump(final_answer, f, ensure_ascii=False)

    print(json.dumps(final_answer, ensure_ascii=False))