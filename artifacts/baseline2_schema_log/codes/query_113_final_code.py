def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter events linked to at least one orders and one customers object
    orders_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'orders']['ocel:oid'])
    customers_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid'])
    filtered_events = [event for event in ocel.events if event['ocel:oid'] in orders_objects and event['ocel:oid'] in customers_objects]
    filtered_ocel = {
        'events': filtered_events,
        'objects': ocel.objects,
        'relations': ocel.relations
    }
    # Step 2: Flatten the restricted OCEL using customers as the case notion
    flattened_log = pm4py.ocel_flattening(filtered_ocel, object_type='customers')
    # Step 3: Select the top 20% variants by frequency
    variant_counts = flattened_log['concept:name'].value_counts()
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()
    top_variant_cases = flattened_log[flattened_log['concept:name'].isin(top_variants)]
    # Step 4: Report the share of cases in that top-variant subset whose case duration exceeds the average case duration
    case_durations = top_variant_cases.groupby('case:concept:name')['time:timestamp'].apply(lambda x: (x.max() - x.min()).total_seconds())
    average_duration = case_durations.mean()
    cases_exceeding_average = (case_durations > average_duration).sum()
    share_exceeding_average = cases_exceeding_average / len(case_durations) if len(case_durations) > 0 else 0
    # Step 5: Discover a Petri net from that top-variant subset
    petri_net = pm4py.discover_petri_net_inductive(top_variant_cases)
    # Step 6: Compute token-based replay fitness of that model on the full restricted flattened view
    fitness = pm4py.fitness_token_based_replay(petri_net, flattened_log)
    # Save outputs
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    with open('output/petri_net.pkl', 'wb') as f:
        pickle.dump(petri_net, f)
    print('OUTPUT_FILE_LOCATION: output/petri_net.pkl')
    final_answer = {
        'share_exceeding_average': share_exceeding_average,
        'fitness': fitness
    }
    print(json.dumps(final_answer, ensure_ascii=False))