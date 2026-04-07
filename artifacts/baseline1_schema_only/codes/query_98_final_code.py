import pm4py
import json
import statistics

def main():
    ocel = ACTIVE_LOG
    # Step 1: Flatten the OCEL to customers view
    flattened_customers = pm4py.ocel_flattening(ocel, 'customers')
    
    # Step 2: Calculate case durations and isolate delayed cases
    case_durations = flattened_customers.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = case_durations['max'] - case_durations['min']
    average_duration = statistics.mean(case_durations['duration'].dt.total_seconds())
    delayed_cases = case_durations[case_durations['duration'].dt.total_seconds() > average_duration].index.tolist()
    
    # Step 3: Map delayed cases back to raw OCEL events
    delayed_events = ocel.events[ocel.events['case:concept:name'].isin(delayed_cases)]
    
    # Step 4: Filter events linked to at least one items object and one customers object
    item_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'items']['ocel:oid'])
    customer_objects = set(ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid'])
    filtered_events = delayed_events[delayed_events['ocel:oid'].isin(item_objects) & delayed_events['ocel:oid'].isin(customer_objects)]
    
    # Step 5: Create a restricted OCEL
    restricted_ocel = pm4py.ocel_create(ocel.objects, filtered_events, ocel.relations)
    
    # Step 6: Flatten the restricted OCEL again using customers as the case notion
    flattened_restricted = pm4py.ocel_flattening(restricted_ocel, 'customers')
    
    # Step 7: Discover the most dominant variant
    variant_counts = flattened_restricted['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    
    # Save the dominant variant to a JSON file
    with open('output/dom_variant_delayed_joint.json', 'w', encoding='utf-8') as f:
        json.dump({'dominant_variant': dominant_variant}, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/dom_variant_delayed_joint.json')
    
    # Final benchmark answer
    final_answer = {'average_case_duration': average_duration, 'delayed_cases_count': len(delayed_cases), 'dominant_variant': dominant_variant}
    print(json.dumps(final_answer, ensure_ascii=False))