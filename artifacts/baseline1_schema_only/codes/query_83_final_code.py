def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for packages
    flat_packages = pm4py.ocel_flattening(ocel, 'packages')
    # Count the frequency of each variant
    variant_counts = flat_packages['concept:name'].value_counts()
    # Select the top 20% variants by frequency
    top_20_percent_count = max(1, math.ceil(len(variant_counts) * 0.2))
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()
    # Filter the flattened log for the top variants
    filtered_log = flat_packages[flat_packages['concept:name'].isin(top_variants)]
    # Discover the DFG
    dfg = pm4py.discover_dfg(filtered_log)
    # Save DFG to CSV
    dfg_df = pd.DataFrame(list(dfg.items()), columns=['source', 'target'])
    dfg_df['count'] = dfg_df['target'].apply(lambda x: dfg[(dfg_df['source'], x)])
    dfg_df.to_csv('output/dfg_top20_packages.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/dfg_top20_packages.csv')
    # Save DFG visualization to PNG
    png_path = 'output/dfg_top20_packages.png'
    pm4py.save_vis_dfg(dfg, png_path)
    print(f'OUTPUT_FILE_LOCATION: {png_path}')  
    # Prepare final answer
    total_edges = sum(dfg.values()) if dfg else 0
    top_edges = [
        {'source': src, 'target': dst, 'count': count, 'share': (count / total_edges if total_edges else 0.0)}
        for (src, dst), count in sorted(dfg.items(), key=lambda x: x[1], reverse=True)[:10]
    ]
    final_answer = {'top_edges': top_edges}
    print(json.dumps(final_answer, ensure_ascii=False))