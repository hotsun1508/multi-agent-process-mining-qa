import os
import json
import pm4py
import pandas as pd


def main():
    event_log = ACTIVE_LOG
    variants_raw = pm4py.get_variants(event_log)

    # Create a dictionary to store the frequency of each variant
    variants_map = {}
    for variant, v in variants_raw.items():
        cnt = v if isinstance(v, int) else len(v)
        if isinstance(variant, (tuple, list)):
            variant_str = ' > '.join(map(str, variant))
        else:
            variant_str = str(variant)
        variants_map[variant_str] = int(cnt)

    # Find the most frequent variant
    most_frequent_variant = max(variants_map, key=variants_map.get)
    frequency = variants_map[most_frequent_variant]

    # Prepare the final answer
    final_answer = {'most_frequent_variant': most_frequent_variant, 'frequency': frequency}

    # Save the final answer to a JSON file
    output_path = 'output/most_frequent_variant.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  

    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()