def setupGraph(O):
    O.addVertex("vertex1", "person", {"field1": "value1", "field2": "value2"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex4", "person")

    O.addEdge("vertex1", "vertex2", "friend", id="edge1")
    O.addEdge("vertex2", "vertex3", "friend", id="edge2")
    O.addEdge("vertex2", "vertex4", "parent", id="edge3")


def test_mark_select(O):
    errors = []

    setupGraph(O)

    for row in O.query().V("vertex1").mark("a").out().mark(
            "b").out().mark("c").select(["a", "b", "c"]):
        if row["a"]["gid"] != "vertex1":
            errors.append("Incorrect as selection")
        if row["a"]["data"] != {"field1": "value1", "field2": "value2"}:
            errors.append("Missing data for selection")
        if row["b"]["gid"] != "vertex2":
            errors.append("Incorrect as selection")
        if row["c"]["gid"] not in ["vertex3", "vertex4"]:
            errors.append("Incorrect as selection")
        if row["c"]["gid"] == "vertex3":
            if row["c"]["data"] != {"field1": "value3", "field2": "value4"}:
                errors.append("Missing data for selection")

    return errors


def test_mark_edge_select(O):
    errors = []

    setupGraph(O)

    for row in O.query().V("vertex1").mark("a").outEdge().mark(
            "b").out().mark("c").select(["a", "b", "c"]):
        if row["a"]["gid"] != "vertex1":
            errors.append("Incorrect as selection")
        if row["b"]["gid"] != "edge1":
            errors.append("Incorrect as edge selection: %s" % row["b"])
        if row["c"]["gid"] != "vertex2":
            errors.append("Incorrect as selection")

    return errors
