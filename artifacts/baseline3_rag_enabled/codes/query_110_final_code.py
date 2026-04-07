import os
import json
import pm4py


def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Filter events linked to at least one orders and one customers object
    filtered_events = [event for event in ocel.events if any(rel['ocel:oid'] == 'orders' for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']) and any(rel['ocel:oid'] == 'customers' for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid'])]
    filtered_ocel = pm4py.objects.ocel.obj.OCEL(events=filtered_events, objects=ocel.objects, relations=ocel.relations)
    
    # Step 2: Flatten the restricted OCEL using orders as the case notion
    flattened_ocel = pm4py.ocel_flattening(filtered_ocel, object_type='orders')
    
    # Step 3: Identify the most dominant variant in the flattened view
    variants = pm4py.get_variants(flattened_ocel)
    dominant_variant = max(variants.items(), key=lambda x: x[1])[0]
    
    # Step 4: Discover a Petri net from the cases of the dominant variant
    dominant_cases = [case for case in flattened_ocel if case['concept:name'] == dominant_variant]
    petri_net = pm4py.discover_petri_net_inductive(dominant_cases)
    
    # Step 5: Run token-based replay on the dominant variant subset
    fitness = pm4py.token_based_replay(petri_net, dominant_cases)
    
    # Step 6: Count events linked to fit cases that are linked to both orders and customers
    fit_cases = [case for case in dominant_cases if fitness[case['case:concept:name']] == 'fit']
    count_joint_events = sum(1 for event in ocel.events if any(rel['ocel:oid'] == 'orders' for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']) and any(rel['ocel:oid'] == 'customers' for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']) and event['ocel:eid'] in fit_cases)
    
    # Save the Petri net visualization
    pm4py.save_vis_petri_net(petri_net, os.path.join(output_dir, 'petri_net.png'))
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'petri_net.png')}')
    
    # Prepare final answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'joint_event_count': count_joint_events,
        'petri_net': 'petri_net.png'
    }
    
    # Save final answer as JSON
    with open(os.path.join(output_dir, 'final_answer.json'), 'w') as f:
        json.dump(final_answer, f, ensure_ascii=False)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'final_answer.json')}')
    
    print(json.dumps(final_answer, ensure_ascii=False))