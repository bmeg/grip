

def test_as_select(O):
    errors = []

    O.addVertex("vertex1", "person", {"field1": "value1", "field2": "value2"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex4", "person")

    O.addEdge("vertex1", "vertex2", "friend")
    O.addEdge("vertex2", "vertex3", "friend")
    O.addEdge("vertex2", "vertex4", "parent")

    for row in O.query().V("vertex1").as_("a").out().as_(
            "b").out().as_("c").select(["a", "b", "c"]).execute():
        res = dict(zip(["a", "b", "c"], row))
        if res["a"]["vertex"]["gid"] != "vertex1":
            errors.append("Incorrect as selection")
        if res["a"]["vertex"]["data"] != {"field1": "value1", "field2": "value2"}:
            errors.append("Missing data for selection")
        if res["b"]["vertex"]["gid"] != "vertex2":
            errors.append("Incorrect as selection")
        if res["c"]["vertex"]["gid"] not in ["vertex3", "vertex4"]:
            errors.append("Incorrect as selection")
        if res["c"]["vertex"]["gid"] == "vertex3":
            if res["c"]["vertex"]["data"] != {"field1": "value3", "field2": "value4"}:
                errors.append("Missing data for selection")

    return errors


def test_as_edge_select(O):
    errors = []

    O.addVertex("vertex1", "person", {"field1": "value1", "field2": "value2"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex4", "person")

    O.addEdge("vertex1", "vertex2", "friend")
    O.addEdge("vertex2", "vertex3", "friend")
    O.addEdge("vertex2", "vertex4", "parent")

    for row in O.query().V("vertex1").as_("a").outEdge().as_(
            "b").out().as_("c").select(["a", "b", "c"]).execute():
        res = dict(zip(["a", "b", "c"], row))
        if res["a"]["vertex"]["gid"] != "vertex1":
            errors.append("Incorrect as selection")
        if "gid" not in res["b"]["edge"]:
            errors.append("Incorrect as edge selection")
        if res["c"]["vertex"]["gid"] not in ["vertex2"]:
            errors.append("Incorrect as selection")

    return errors
