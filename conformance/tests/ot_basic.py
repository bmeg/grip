

def test_count(O):
    errors = []

    O.addVertex("vertex1", "person", {"field1" : "value1", "field2" : "value2"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person", {"field1" : "value3", "field2" : "value4"})
    O.addVertex("vertex4", "person")

    O.addEdge("vertex1", "vertex2", "friend")
    O.addEdge("vertex2", "vertex3", "friend")
    O.addEdge("vertex2", "vertex4", "parent")

    count = 0
    for i in O.query().V().execute():
        count += 1
    if count != 4:
        errors.append("Fail: O.query().V() %s != %s" % (count, 4))

    count = 0
    for i in O.query().E().execute():
        count += 1
    if count != 3:
        errors.append("Fail: O.query().E()")

    count = 0
    for i in O.query().V("vertex1").outgoing().execute():
        count += 1
    if count != 1:
        errors.append("Fail: O.query().V(\"vertex1\").outgoing() %s != %d" % (count, 1))

    count = 0
    for i in O.query().V("vertex1").outgoing().outgoing().has("field2", "value4").incoming().execute():
        count += 1
    if count != 1:
        errors.append("""O.query().V("vertex1").outgoing().outgoing().has("field1", "value4")""")

    return errors

def test_outgoing(O):
    errors = []

    O.addVertex("vertex1", "person", {"field1" : "value1", "field2" : "value2"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person", {"field1" : "value3", "field2" : "value4"})
    O.addVertex("vertex4", "person")

    O.addEdge("vertex1", "vertex2", "friend")
    O.addEdge("vertex2", "vertex3", "friend")
    O.addEdge("vertex2", "vertex4", "parent")

    if O.query().V("vertex2").outgoing().count().first()["data"] != 2:
        errors.append("blank outgoing doesn't work")

    if O.query().V("vertex2").outgoing("friend").count().first()["data"] != 1:
        errors.append("labeled outgoing doesn't work")

    if O.query().V("vertex2").incoming().count().first()["data"] != 1:
        errors.append("blank incoming doesn't work")

    return errors


def test_both(O):
    errors = []

    O.addVertex("vertex1", "person")
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person")
    O.addVertex("vertex4", "person")

    O.addEdge("vertex1", "vertex2", "friend")
    O.addEdge("vertex1", "vertex3", "friend")
    O.addEdge("vertex4", "vertex1", "parent")

    count = 0
    for row in O.query().V("vertex1").both().both().execute():
        count += 1
        if row['vertex']['gid'] != "vertex1":
            error.append("Wrong vertex found: " % (row['vector']['gid']))
    if count != 3:
        error.append("Wrong vertex count found %s != %s" % (count, 3))

    return errors


def test_duplicate(O):
    errors = []
    O.addVertex("vertex1", "person", {"data": 1})
    O.addVertex("vertex1", "person")
    O.addVertex("vertex2", "person")

    O.addEdge("vertex1", "vertex2", "friend")
    O.addEdge("vertex1", "vertex2", "friend")

    if O.query().V().count().first()['data'] != 2:
        errors.append("duplicate vertex add error")

    if O.query().E().count().first()['data'] != 2:
        errors.append("duplicate edge add error")
    return errors
