import json
import collections
import pm4py


def main():
    ocel = ACTIVE_LOG
    # Step 1: Filter the OCEL to events linked to at least one orders object and at least one customers object
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
    activity_durations = collections.defaultdict(list)
    for case in flattened_ocel:
        case_name = case['case:concept:name']
        for event in case['events']:
            activity = event['concept:name']
            timestamp = event['time:timestamp']
            activity_durations[activity].append(timestamp)
    mean_sojourn_times = {activity: (max(times) - min(times)).total_seconds() / len(times) for activity, times in activity_durations.items() if len(times) > 1}
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
    final_answer = {'longest_activity': longest_activity, 'mean_sojourn_time': longest_activity_time}
    print(json.dumps(final_answer, ensure_ascii=False))