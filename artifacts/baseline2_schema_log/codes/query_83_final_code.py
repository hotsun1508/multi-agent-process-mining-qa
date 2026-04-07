def main():
    ocel = ACTIVE_LOG
    flattened_packages = pm4py.ocel_flattening(ocel, 'packages')
    variant_counts = flattened_packages['concept:name'].value_counts()
    top_20_percent_count = max(1, math.ceil(len(variant_counts) * 0.2))
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()
    filtered_log = flattened_packages[flattened_packages['concept:name'].isin(top_variants)]
    dfg = pm4py.discover_dfg(filtered_log)
    dfg_csv_path = 'output/dfg_top20_packages.csv'
    dfg_png_path = 'output/dfg_top20_packages.png'
    start_activities = {activity: count for activity, count in filtered_log['concept:name'].value_counts().items()}
    end_activities = {activity: count for activity, count in filtered_log['concept:name'].value_counts().items()}
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_png_path)
    pd.DataFrame.from_dict(dfg, orient='index').reset_index().to_csv(dfg_csv_path, header=['source', 'target', 'count'], index=False)
    print(f'OUTPUT_FILE_LOCATION: {dfg_csv_path}')
    print(f'OUTPUT_FILE_LOCATION: {dfg_png_path}')
    total = sum(dfg.values()) if dfg else 0
    top_edges = [
        {'source': src, 'target': dst, 'count': count, 'share': (count / total if total else 0.0)}
        for (src, dst), count in sorted(dfg.items(), key=lambda x: x[1], reverse=True)[:10]
    ]
    final_answer = {'top_edges': top_edges}
    print(json.dumps(final_answer, ensure_ascii=False))