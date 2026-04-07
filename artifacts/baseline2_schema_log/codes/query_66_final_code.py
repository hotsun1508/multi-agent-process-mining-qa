def main():
    ocel = ACTIVE_LOG
    flattened_items = pm4py.ocel_flattening(ocel, 'items')
    variant_counts = flattened_items['concept:name'].value_counts()
    total_cases = flattened_items['case:concept:name'].nunique()
    top_1_percent_count = max(1, int(len(variant_counts) * 0.01))
    top_variants = variant_counts.nlargest(top_1_percent_count)
    covered_cases = flattened_items[flattened_items['concept:name'].isin(top_variants.index)]['case:concept:name'].nunique()
    coverage_percentage = (covered_cases / total_cases) * 100
    result = coverage_percentage > 20
    with open('output/top1_covers20_items.json', 'w') as f:
        json.dump(result, f)
    print('OUTPUT_FILE_LOCATION: output/top1_covers20_items.json')
    final_answer = {'top1_covers_20_percent': result}
    print(json.dumps(final_answer, ensure_ascii=False))