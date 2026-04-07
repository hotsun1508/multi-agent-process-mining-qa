def main():
    ocel = ACTIVE_LOG
    # Extract the o2o relationships from the OCEL
    o2o_relationships = ocel.relations

    # Filter for the specific object types 'customers' and 'employees'
    filtered_relationships = o2o_relationships[
        (o2o_relationships['ocel:type'] == 'customers') | 
        (o2o_relationships['ocel:type'] == 'employees')
    ]

    # Extract the relationship qualifiers
    relationship_qualifiers = filtered_relationships['ocel:qualifier'].unique().tolist()

    # Construct the result dictionary
    result = {
        "primary_answer_in_csv_log": True,
        "result_type": "single",
        "view": "raw_ocel_or_flattened_view_as_specified",
        "result_schema": {"object_interaction": "table"},
        "artifacts_schema": ["output/* (optional auxiliary artifacts such as png/csv/pkl/json)"],
        "relationship_qualifiers": relationship_qualifiers
    }

    # Save the result dictionary as a JSON file
    output_path = 'output/relationship_qualifiers.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  

    # Return the result dictionary
    print(json.dumps(result, ensure_ascii=False))