

def test_count(O):
    errors = []

    O.addVertex("vertex1", "person", {"field1": "value1", "field2": "value2"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex4", "person")

    O.addEdge("vertex1", "vertex2", "friend", id="edge1")
    O.addEdge("vertex2", "vertex3", "friend", id="edge2")
    O.addEdge("vertex2", "vertex4", "parent", id="edge3")

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
        errors.append(
            "Fail: O.query().V(\"vertex1\").outgoing() %s != %d" % (count, 1))

    count = 0
    for i in O.query().V("vertex1").outgoing().outgoing().has(
            "field2", "value4").incoming().execute():
        count += 1
    if count != 1:
        errors.append(
            """O.query().V("vertex1").outgoing().outgoing().has("field1", "value4") : %s != %s""" %
            (count, 1))

    count = 0
    for i in O.query().E("edge1").execute():
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().E(\"edge1\") %s != %d" % (count, 1))

    count = 0
    for i in O.query().E("edge1").outgoing().execute():
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().E(\"edge1\").outgoing() %s != %d" % (count, 1))

    count = 0
    for i in O.query().E("edge1").incoming().execute():
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().E(\"edge1\").incoming() %s != %d" % (count, 1))

    return errors


def test_outgoing(O):
    errors = []

    O.addVertex("vertex1", "person", {"field1": "value1", "field2": "value2"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person", {"field1": "value3", "field2": "value4"})
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

    for i in O.query().V("vertex2").outgoing():
        if i['vertex']['gid'] not in ["vertex3", "vertex4"]:
            errors.append("Wrong outgoing vertex %s" % (i['vertex']['gid']))

    if O.query().V("vertex2").outgoing("friend").count().first()["data"] != 1:
        errors.append("labeled outgoing doesn't work")

    if O.query().V("vertex2").incoming().count().first()["data"] != 1:
        errors.append("blank incoming doesn't work")

    return errors


def test_incoming(O):
    errors = []

    O.addVertex("vertex1", "person", {"field1": "value1", "field2": "value2"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex4", "person")

    O.addEdge("vertex1", "vertex2", "friend")
    O.addEdge("vertex2", "vertex3", "friend")
    O.addEdge("vertex2", "vertex4", "parent")

    if O.query().V("vertex2").incoming().count().first()["data"] != 1:
        errors.append("blank incoming doesn't work")

    for i in O.query().V("vertex4").incoming():
        if i['vertex']['gid'] not in ["vertex2"]:
            errors.append("Wrong incoming vertex %s" % (i['vertex']['gid']))

    if O.query().V("vertex3").incoming("friend").count().first()["data"] != 1:
        errors.append("labeled incoming doesn't work")

    return errors


def test_outgoing_edge(O):
    errors = []

    O.addVertex("vertex1", "person", {"field1": "value1", "field2": "value2"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex4", "person")

    O.addEdge("vertex1", "vertex2", "friend")
    O.addEdge("vertex2", "vertex3", "friend", id="edge1")
    O.addEdge("vertex2", "vertex4", "parent", id="edge2")

    if O.query().V("vertex2").outgoingEdge().count().first()["data"] != 2:
        errors.append("blank outgoing doesn't work")

    for i in O.query().V("vertex2").outgoingEdge():
        if i['edge']['gid'] not in ["edge1", "edge2"]:
            errors.append("Wrong outgoing vertex %s" % (i['edge']['gid']))

    if O.query().V("vertex2").outgoingEdge(
            "friend").count().first()["data"] != 1:
        errors.append("labeled outgoing doesn't work")

    if O.query().V("vertex2").incomingEdge().count().first()["data"] != 1:
        errors.append("blank incoming doesn't work")

    return errors


def test_incoming_edge(O):
    errors = []

    O.addVertex("vertex1", "person", {"field1": "value1", "field2": "value2"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex4", "person")

    O.addEdge("vertex1", "vertex2", "friend")
    O.addEdge("vertex2", "vertex3", "friend", id="edge1")
    O.addEdge("vertex2", "vertex4", "parent", id="edge2")

    if O.query().V("vertex2").incomingEdge().count().first()["data"] != 1:
        errors.append("blank incoming doesn't work")

    for i in O.query().V("vertex4").incomingEdge():
        if i['edge']['gid'] not in ["edge2"]:
            errors.append("Wrong incoming vertex %s" % (i['edge']['gid']))

    if O.query().V("vertex3").incomingEdge(
            "friend").count().first()["data"] != 1:
        errors.append("labeled incoming doesn't work")

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

    # time.sleep(1)

    count = 0
    for row in O.query().V("vertex1").both().both().execute():
        count += 1
        if row['vertex']['gid'] != "vertex1":
            errors.append("Wrong vertex found: %s" % (row['vertex']['gid']))
    if count != 3:
        errors.append("Wrong vertex count found %s != %s" % (count, 3))

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
