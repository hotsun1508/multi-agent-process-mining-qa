def main():
    ocel = ACTIVE_LOG
    flattened_items = pm4py.ocel_flattening(ocel, object_type='items')
    variant_counts = flattened_items['concept:name'].value_counts()
    total_cases = flattened_items['case:concept:name'].nunique()
    top_1_percent_count = max(1, math.ceil(len(variant_counts) * 0.01))
    top_variants = variant_counts.nlargest(top_1_percent_count)
    covered_cases = flattened_items[flattened_items['concept:name'].isin(top_variants.index)]['case:concept:name'].nunique()
    coverage_percentage = covered_cases / total_cases > 0.2
    with open('output/top1_covers20_items.json', 'w') as f:
        json.dump(coverage_percentage, f)
    print('OUTPUT_FILE_LOCATION: output/top1_covers20_items.json')
    final_answer = {'top1_covers_20_percent': coverage_percentage}
    print(json.dumps(final_answer, ensure_ascii=False))