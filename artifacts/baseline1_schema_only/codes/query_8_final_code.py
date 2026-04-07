import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log)
    variant_counts = log_df['case:concept:name'].value_counts()
    total_cases = len(log_df['case:concept:name'].unique())
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.head(top_20_percent_count)
    covered_cases = top_variants.sum()
    coverage_percentage = (covered_cases / total_cases) * 100
    result = coverage_percentage >= 80
    final_answer = {'top_20_percent_variants_cover_80_percent_cases': result}

    # Save the result to a JSON file
    output_path = 'output/benchmark_results.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  

    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()