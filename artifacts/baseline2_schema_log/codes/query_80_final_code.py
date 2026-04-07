def main():
    ocel = ACTIVE_LOG
    ocdfg = pm4py.discover_ocdfg(ocel)
    edge_tables = ocdfg["edges"]["event_couples"]
    top_rows = []
    for object_type, edge_map in edge_tables.items():
        for (src, dst), linked_pairs in edge_map.items():
            top_rows.append({
                "object_type": object_type,
                "source": src,
                "target": dst,
                "count": len(linked_pairs),
            })
    top_rows = sorted(top_rows, key=lambda row: row["count"], reverse=True)[:10]
    top_arity_events = pd.DataFrame(top_rows)
    top_arity_events.to_csv("output/top_arity_events.csv", index=False)
    print("OUTPUT_FILE_LOCATION: output/top_arity_events.csv")
    total_nodes = len(ocdfg["activities"])
    total_edges = sum(len(edge_map) for edge_map in edge_tables.values())
    final_answer = {
        "graph_type": "ocdfg",
        "total_nodes": total_nodes,
        "total_edges": total_edges,
        "top_events": top_rows,
    }
    print(json.dumps(final_answer, ensure_ascii=False))