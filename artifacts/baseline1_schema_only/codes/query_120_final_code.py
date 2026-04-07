import pm4py
import json
import statistics


def main():
    ocel = ACTIVE_LOG
    # Step 1: Flatten the OCEL to get the items view
    flattened_items = pm4py.ocel_flattening(ocel, 'items')
    
    # Step 2: Identify the most dominant variant in the flattened items view
    variant_counts = flattened_items['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    
    # Step 3: Filter the raw OCEL for events linked to the dominant variant
    dominant_cases = flattened_items[flattened_items['concept:name'] == dominant_variant]['case:concept:name'].unique()
    
    # Count events linked to both items and orders
    raw_events = ocel.events
    joint_event_count = len(raw_events[(raw_events['ocel:oid'].isin(dominant_cases)) & (raw_events['ocel:type'].isin(['items', 'orders']))])
    
    # Step 4: Calculate average case duration in the full flattened view
    case_durations = flattened_items.groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).tolist()
    average_duration = statistics.mean(case_durations)
    
    # Step 5: Filter cases whose duration exceeds the average duration
    long_duration_cases = flattened_items[flattened_items['case:concept:name'].isin(dominant_cases)]
    long_duration_cases = long_duration_cases.groupby('case:concept:name').filter(lambda x: (x['time:timestamp'].max() - x['time:timestamp'].min()).total_seconds() > average_duration)
    
    # Step 6: Discover a Petri net from the delayed dominant-variant subset
    petri_net = pm4py.discover_petri_net_inductive(long_duration_cases)
    
    # Step 7: Compute token-based replay fitness of the model on the full flattened items view
    fitness = pm4py.fitness_token_based_replay(petri_net, flattened_items)
    
    # Save outputs
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    with open('output/petri_net.pkl', 'wb') as f:
        pickle.dump(petri_net, f)
    print('OUTPUT_FILE_LOCATION: output/petri_net.pkl')
    
    # Final answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'joint_event_count': joint_event_count,
        'average_case_duration': average_duration,
        'fitness': fitness
    }
    print(json.dumps(final_answer, ensure_ascii=False))