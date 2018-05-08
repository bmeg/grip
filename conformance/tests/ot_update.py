def test_duplicate(O):
    errors = []
    O.addVertex("vertex1", "person", {"somedata": 1})
    O.addVertex("vertex1", "person")
    O.addVertex("vertex1", "clone", {"otherdata": "foo"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex2", "clone")

    O.addEdge("vertex1", "vertex2", "friend", data={"field": 1}, id="edge1")
    O.addEdge("vertex1", "vertex2", "friend", id="edge1")
    O.addEdge("vertex1", "vertex2", "friend", data={"weight": 5}, id="edge1")

    if list(O.query().V().count().execute())[0]["data"] != 2:
        errors.append("duplicate vertex add error")

    if list(O.query().E().count().execute())[0]["data"] != 1:
        errors.append("duplicate edge add error")

    return errors


def test_replace(O):
    errors = []
    O.addVertex("vertex1", "person", {"somedata": 1})
    O.addVertex("vertex1", "person")
    O.addVertex("vertex1", "clone", {"otherdata": "foo"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex2", "clone")

    O.addEdge("vertex1", "vertex2", "friend", data={"field": 1}, id="edge1")
    O.addEdge("vertex1", "vertex2", "friend", id="edge1")
    O.addEdge("vertex1", "vertex2", "friend", data={"weight": 5}, id="edge1")

    if O.getVertex("vertex1")["label"] != "clone":
        errors.append("vertex has unexpected label")

    if O.getVertex("vertex1")["data"] != {"otherdata": "foo"}:
        errors.append("vertex has unexpected data")

    if O.getEdge("edge1")["data"] != {"weight": 5}:
        errors.append("edge is missing expected data")

    return errors
