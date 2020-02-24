def test_duplicate(O, man):

    man.writeTest()

    errors = []
    O.addVertex("vertex1", "person", {"somedata": 1})
    O.addVertex("vertex1", "person")
    O.addVertex("vertex1", "clone", {"otherdata": "foo"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex2", "clone")

    O.addEdge("vertex1", "vertex2", "friend", data={"field": 1}, gid="edge1")
    O.addEdge("vertex1", "vertex2", "friend", gid="edge1")
    O.addEdge("vertex1", "vertex2", "friend", data={"weight": 5}, gid="edge1")

    if O.query().V().count().execute()[0]["count"] != 2:
        errors.append("duplicate vertex add error")

    if O.query().E().count().execute()[0]["count"] != 1:
        errors.append("duplicate edge add error")

    return errors


def test_replace(O, man):

    man.writeTest()

    errors = []
    O.addVertex("vertex1", "person", {"somedata": 1})
    O.addVertex("vertex1", "person")
    O.addVertex("vertex1", "clone", {"otherdata": "foo"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex2", "clone")

    O.addEdge("vertex1", "vertex2", "friend", data={"field": 1}, gid="edge1")
    O.addEdge("vertex1", "vertex2", "friend", gid="edge1")
    O.addEdge("vertex1", "vertex2", "friend", data={"weight": 5}, gid="edge1")

    if O.getVertex("vertex1")["label"] != "clone":
        errors.append("vertex has unexpected label")

    if O.getVertex("vertex1")["data"] != {"otherdata": "foo"}:
        errors.append("vertex has unexpected data")

    if O.getEdge("edge1")["data"] != {"weight": 5}:
        errors.append("edge is missing expected data")

    return errors


def test_delete(O):
    errors = []

    O.addVertex("vertex1", "person", {"field1": "value1", "field2": "value2"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex4", "person")

    O.addEdge("vertex1", "vertex2", "friend", gid="edge1")
    O.addEdge("vertex2", "vertex3", "friend", gid="edge2")
    O.addEdge("vertex2", "vertex4", "parent", gid="edge3")

    count = 0
    for i in O.query().V():
        count += 1
    if count != 4:
        errors.append("Fail: O.query().V() %s != %s" % (count, 4))

    count = 0
    for i in O.query().E():
        count += 1
    if count != 3:
        errors.append("Fail: O.query().E()")

    O.deleteVertex("vertex1")
    count = 0
    for i in O.query().V():
        count += 1
    if count != 3:
        errors.append(
            "Fail: O.query().V() %s != %d" % (count, 3))

    count = 0
    for i in O.query().E():
        count += 1
    if count != 2:
        errors.append(
            "Fail: O.query().E() %s != %d" % (count, 2))

    O.deleteEdge("edge2")
    count = 0
    for i in O.query().E():
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().E() %s != %d" % (count, 1))

    return errors
