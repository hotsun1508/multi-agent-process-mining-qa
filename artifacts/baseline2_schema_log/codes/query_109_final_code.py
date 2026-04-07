def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter events linked to at least one orders and one customers object
    orders_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'orders']['ocel:oid'])
    customers_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid'])
    filtered_events = [event for event in ocel.events if any(rel['ocel:oid'] in orders_objects for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']) and any(rel['ocel:oid'] in customers_objects for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid'])]
    filtered_ocel = {
        'events': filtered_events,
        'objects': ocel.objects,
        'relations': ocel.relations
    }
    
    # Step 2: Flatten the restricted OCEL using orders as the case notion
    flattened_log = pm4py.ocel_flattening(filtered_ocel, object_type='orders')
    
    # Step 3: Select the top 20% variants by frequency
    variant_counts = flattened_log['concept:name'].value_counts()
    top_20_percent_threshold = variant_counts.quantile(0.8)
    top_variants = variant_counts[variant_counts >= top_20_percent_threshold].index.tolist()
    model_building_sublog = flattened_log[flattened_log['concept:name'].isin(top_variants)]
    
    # Step 4: Discover a Petri net from that sublog
    petri_net = pm4py.discover_petri_net_inductive(model_building_sublog)
    
    # Step 5: Run token-based replay and report the number of unique variants among the fit cases
    fit_cases = pm4py.replay_fitness(petri_net, model_building_sublog)
    unique_fit_variants = len(set(fit_cases['variant']))
    
    # Step 6: Save outputs
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    with open('output/petri_net.pkl', 'wb') as f:
        pickle.dump(petri_net, f)
    print('OUTPUT_FILE_LOCATION: output/petri_net.pkl')
    
    # Final answer
    final_answer = {
        'unique_fit_variants': unique_fit_variants,
        'top_variants_count': len(top_variants)
    }
    print(json.dumps(final_answer, ensure_ascii=False))