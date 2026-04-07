def main():
    ocel = ACTIVE_LOG
    relations = ocel.relations
    customer_employee_relationships = relations[(relations['ocel:oid'].str.contains('customer')) & (relations['ocel:oid'].str.contains('employee'))]
    relationship_qualifiers = customer_employee_relationships['ocel:qualifier'].unique().tolist()
    final_answer = {'relationship_qualifiers': relationship_qualifiers}
    with open('output/relationship_qualifiers.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/relationship_qualifiers.json')
    print(json.dumps(final_answer, ensure_ascii=False))