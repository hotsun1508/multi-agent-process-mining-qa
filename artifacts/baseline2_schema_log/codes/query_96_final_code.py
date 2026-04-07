def main():
    ocel = ACTIVE_LOG
    flattened_packages = pm4py.ocel_flattening(ocel, object_type='packages')
    dfg, start_activities, end_activities = pm4py.discover_dfg(flattened_packages)
    total_events = sum(dfg.values()) if dfg else 0
    most_frequent_edge = max(dfg.items(), key=lambda x: x[1])
    source, target = most_frequent_edge[0]
    edge_count = most_frequent_edge[1]
    cases_with_edge = flattened_packages[flattened_packages['concept:name'].shift() == source][flattened_packages['concept:name'] == target]
    case_ids = cases_with_edge['case:concept:name'].unique()
    case_durations = flattened_packages[flattened_packages['case:concept:name'].isin(case_ids)].groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds())
    average_case_duration = case_durations.mean()
    variants = flattened_packages.groupby('case:concept:name')['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    dominant_variant = variants.idxmax()
    edge_subset_stats = {
        'most_frequent_edge': {'source': source, 'target': target, 'count': edge_count},
        'dominant_variant': dominant_variant,
        'average_case_duration': average_case_duration
    }
    with open('output/edge_subset_stats.json', 'w', encoding='utf-8') as f:
        json.dump(edge_subset_stats, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/edge_subset_stats.json')
    final_answer = {
        'most_frequent_edge': edge_subset_stats['most_frequent_edge'],
        'dominant_variant': edge_subset_stats['dominant_variant'],
        'average_case_duration': edge_subset_stats['average_case_duration']
    }
    print(json.dumps(final_answer, ensure_ascii=False))