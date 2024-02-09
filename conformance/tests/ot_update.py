def test_duplicate(man):

    G = man.writeTest()

    errors = []
    G.addVertex("vertex1", "person", {"somedata": 1})
    G.addVertex("vertex1", "person")
    G.addVertex("vertex1", "clone", {"otherdata": "foo"})
    G.addVertex("vertex2", "person")
    G.addVertex("vertex2", "clone")

    G.addEdge("vertex1", "vertex2", "friend", data={"field": 1}, gid="edge1")
    G.addEdge("vertex1", "vertex2", "friend", gid="edge1")
    G.addEdge("vertex1", "vertex2", "friend", data={"weight": 5}, gid="edge1")

    if G.query().V().count().execute()[0]["count"] != 2:
        errors.append("duplicate vertex add error")

    if G.query().E().count().execute()[0]["count"] != 1:
        errors.append("duplicate edge add error")

    return errors


def test_replace(man):

    G = man.writeTest()

    errors = []
    G.addVertex("vertex1", "person", {"somedata": 1})
    G.addVertex("vertex1", "person")
    G.addVertex("vertex1", "clone", {"otherdata": "foo"})
    G.addVertex("vertex2", "person")
    G.addVertex("vertex2", "clone")

    G.addEdge("vertex1", "vertex2", "friend", data={"field": 1}, gid="edge1")
    G.addEdge("vertex1", "vertex2", "friend", gid="edge1")
    G.addEdge("vertex1", "vertex2", "friend", data={"weight": 5}, gid="edge1")

    if G.getVertex("vertex1")["label"] != "clone":
        errors.append("vertex has unexpected label")

    if G.getVertex("vertex1")["data"] != {"otherdata": "foo"}:
        errors.append("vertex has unexpected data")

    if G.getEdge("edge1")["data"] != {"weight": 5}:
        errors.append("edge is missing expected data: %s" % (G.getEdge("edge1")))

    return errors


def test_delete(man):
    errors = []

    G = man.writeTest()

    G.addVertex("vertex1", "person", {"field1": "value1", "field2": "value2"})
    G.addVertex("vertex2", "person")
    G.addVertex("vertex3", "person", {"field1": "value3", "field2": "value4"})
    G.addVertex("vertex4", "person")

    G.addEdge("vertex1", "vertex2", "friend", gid="edge1")
    G.addEdge("vertex2", "vertex3", "friend", gid="edge2")
    G.addEdge("vertex2", "vertex4", "parent", gid="edge3")

    count = 0
    for i in G.query().V():
        count += 1
    if count != 4:
        errors.append("Fail: G.query().V() %s != %s" % (count, 4))

    count = 0
    for i in G.query().E():
        count += 1
    if count != 3:
        errors.append("Fail: G.query().E()")

    G.deleteVertex("vertex1")
    count = 0
    for i in G.query().V():
        count += 1
    if count != 3:
        errors.append(
            "Fail: G.query().V() %s != %d" % (count, 3))

    count = 0
    for i in G.query().E():
        count += 1
    if count != 2:
        errors.append(
            "Fail: G.query().E() %s != %d" % (count, 2))

    G.deleteEdge("edge2")
    count = 0
    for i in G.query().E():
        count += 1
    if count != 1:
        errors.append(
            "Fail: G.query().E() %s != %d" % (count, 1))

    return errors



def test_delete_edge(man):
    """
    Ensure that if a vertex is removed, that the connected edges are deleted as well
    """
    errors = []

    G = man.writeTest()

    G.addVertex("vertex1", "person", {"field1": "value1", "field2": "value2"})
    G.addVertex("vertex2", "person")
    G.addVertex("vertex3", "person", {"field1": "value3", "field2": "value4"})

    G.addEdge("vertex1", "vertex2", "friend", gid="edge1")
    G.addEdge("vertex2", "vertex3", "friend", gid="edge2")

    count = 0
    for i in G.query().V():
        count += 1
    if count != 3:
        errors.append("Fail: G.query().V() %s != %s" % (count, 3))

    count = 0
    for i in G.query().E():
        count += 1
    if count != 2:
        errors.append("Fail: G.query().E()")

    G.deleteVertex("vertex2")

    count = 0
    for i in G.query().V("vertex1").outE():
        count += 1
    if count != 0:
        errors.append(
            "Fail: G.query().V(\"vertex1\").outE() %s != %d" % (count, 0))

    count = 0
    for i in G.query().V("vertex3").inE():
        count += 1
    if count != 0:
        errors.append(
            "Fail: G.query().V(\"vertex3\").inE() %s != %d" % (count, 0))

    return errors
