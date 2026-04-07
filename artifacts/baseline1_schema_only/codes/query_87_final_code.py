def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter events linked to at least one orders object and at least one customers object
    orders_objects = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'orders'}
    customers_objects = {obj['ocel:oid'] for obj in ocel.objects if obj['ocel:type'] == 'customers'}
    filtered_events = [event for event in ocel.events if any(rel['ocel:oid'] in orders_objects for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid']) and any(rel['ocel:oid'] in customers_objects for rel in ocel.relations if rel['ocel:eid'] == event['ocel:eid'])]
    filtered_ocel = {
        'ocel:events': filtered_events,
        'ocel:objects': ocel.objects,
        'ocel:relations': ocel.relations
    }
    
    # Step 2: Flatten the restricted OCEL using customers as the case notion
    flattened_ocel = pm4py.ocel_flattening(filtered_ocel, object_type='customers')
    
    # Step 3: Calculate mean sojourn time for each activity
    activity_times = {}
    activity_counts = {}
    for index, row in flattened_ocel.iterrows():
        activity = row['concept:name']
        timestamp = row['time:timestamp']
        if activity not in activity_times:
            activity_times[activity] = 0
            activity_counts[activity] = 0
        activity_times[activity] += timestamp
        activity_counts[activity] += 1
    mean_sojourn_times = {activity: activity_times[activity] / activity_counts[activity] for activity in activity_times}
    
    # Step 4: Find the activity with the largest mean sojourn time
    longest_activity = max(mean_sojourn_times, key=mean_sojourn_times.get)
    longest_activity_time = mean_sojourn_times[longest_activity]
    
    # Save the result
    with open('output/longest_activity_orders_customers_customers.json', 'w') as f:
        json.dump({
            'activity': longest_activity,
            'mean_sojourn_time': longest_activity_time
        }, f)
    print('OUTPUT_FILE_LOCATION: output/longest_activity_orders_customers_customers.json')
    
    # Final benchmark answer
    final_answer = {
        'longest_activity': longest_activity,
        'mean_sojourn_time': longest_activity_time
    }
    print(json.dumps(final_answer, ensure_ascii=False))