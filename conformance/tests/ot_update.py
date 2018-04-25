def test_duplicate(O):
    errors = []
    O.addVertex("vertex1", "person", {"data": 1})
    O.addVertex("vertex1", "person")
    O.addVertex("vertex2", "person")

    O.addEdge("vertex1", "vertex2", "friend")
    O.addEdge("vertex1", "vertex2", "friend")

    if O.query().V().count().first()["data"] != 2:
        errors.append("duplicate vertex add error")

    if O.query().E().count().first()["data"] != 2:
        errors.append("duplicate edge add error")

    return errors


def test_replace(O):
    errors = []
    O.addVertex("vertex1", "person", {"data": 1})
    O.addVertex("vertex1", "person")
    O.addVertex("vertex2", "person")

    O.addEdge("vertex1", "vertex2", "friend", id="edge1")
    O.addEdge("vertex1", "vertex2", "friend", data={"weight": 5}, id="edge1")

    if O.getVertex("vertex1")["data"] != {}:
        errors.append("vertex has unexpected data")

    if O.getEdge("edge1")["data"] != {"weight": 5}:
        errors.append("edge is missing expected data")

    return errors
