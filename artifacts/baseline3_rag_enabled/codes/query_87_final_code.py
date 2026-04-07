import os
import pandas as pd
import pm4py
import json
from pm4py.objects.ocel.obj import OCEL


def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Filter events linked to 'customers' and 'orders'
    customer_ids = ocel.objects[ocel.objects['ocel:type'] == 'customers']['ocel:oid']
    order_ids = ocel.objects[ocel.objects['ocel:type'] == 'orders']['ocel:oid']
    
    # Get events linked to both customers and orders
    filtered_events = ocel.events[ocel.events['ocel:oid'].isin(customer_ids) & ocel.events['ocel:oid'].isin(order_ids)]
    
    # Create a restricted OCEL
    restricted_ocel = OCEL(events=filtered_events, objects=ocel.objects, relations=ocel.relations)
    
    # Step 2: Flatten the restricted OCEL using customers as the case notion
    flattened_ocel = pm4py.ocel_flattening(restricted_ocel, object_type='customers')
    
    # Step 3: Calculate mean sojourn time for each activity
    activity_durations = {}  # Dictionary to hold total duration and count of occurrences
    for case in flattened_ocel:
        case_activities = case['concept:name']
        timestamps = case['time:timestamp']
        for i in range(len(case_activities) - 1):
            activity = case_activities[i]
            duration = (timestamps[i + 1] - timestamps[i]).total_seconds()
            if activity not in activity_durations:
                activity_durations[activity] = {'total_duration': 0, 'count': 0}
            activity_durations[activity]['total_duration'] += duration
            activity_durations[activity]['count'] += 1
    
    # Calculate mean sojourn time for each activity
    mean_sojourn_times = {activity: data['total_duration'] / data['count'] for activity, data in activity_durations.items()}
    
    # Step 4: Identify the activity with the largest mean sojourn time
    longest_activity = max(mean_sojourn_times, key=mean_sojourn_times.get)
    longest_activity_time = mean_sojourn_times[longest_activity]
    
    # Save the result
    result_data = {'activity': longest_activity, 'mean_sojourn_time': longest_activity_time}
    with open(os.path.join(output_dir, 'longest_activity_orders_customers_customers.json'), 'w') as f:
        json.dump(result_data, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'longest_activity_orders_customers_customers.json')}')
    
    # Final benchmark answer
    final_answer = {
        'primary_answer_in_csv_log': True,
        'result_type': 'composite',
        'view': 'raw_ocel_or_flattened_view_as_specified',
        'result_schema': {'performance': result_data},
        'artifacts_schema': ['output/* (optional auxiliary artifacts such as png/csv/pkl/json)']
    }
    print(json.dumps(final_answer, ensure_ascii=False))