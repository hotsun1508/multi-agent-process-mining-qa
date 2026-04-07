def main():
    ocel = ACTIVE_LOG
    flattened_packages = pm4py.ocel_flattening(ocel, object_type='packages')
    trace_variants = flattened_packages.groupby(['case:concept:name'])['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    most_frequent_variant = trace_variants.idxmax()
    frequency = trace_variants.max()
    total_variants = trace_variants.sum()
    relative_share = frequency / total_variants if total_variants > 0 else 0.0
    result = {'variant': most_frequent_variant, 'frequency': frequency, 'relative_share': relative_share}
    with open('output/most_frequent_variant_packages.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/most_frequent_variant_packages.json')
    final_answer = {'most_frequent_variant': most_frequent_variant, 'frequency': frequency, 'relative_share': relative_share}
    print(json.dumps(final_answer, ensure_ascii=False))