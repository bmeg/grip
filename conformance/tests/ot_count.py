def setupGraph(O):
    O.addVertex("vertex1", "person", {"field1": "value1", "field2": "value2"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex4", "person")

    O.addEdge("vertex1", "vertex2", "friend", gid="edge1")
    O.addEdge("vertex2", "vertex3", "friend", gid="edge2")
    O.addEdge("vertex2", "vertex4", "parent", gid="edge3")


def test_count(O, man):
    errors = []

    man.setGraph("graph1")

    i = list(O.query().V().count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.query().V().count()")
    elif i[0].count != 4:
        errors.append("Fail: O.query().V().count() %s != %s" % (i[0].count, 4))

    i = list(O.query().V("non-existent").count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.query().V(\"non-existent\").count()")
    elif i[0].count != 0:
        errors.append("Fail: O.query().V(\"non-existent\").count() %s != %s" % (i[0].count, 0))

    i = list(O.query().E().count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.query().E().count()")
    elif i[0].count != 3:
        errors.append("Fail: O.query().E().count() %s != %s" % (i[0].count, 3))

    i = list(O.query().E("non-existent").count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.query().E(\"non-existent\").count()")
    elif i[0].count != 0:
        errors.append("Fail: O.query().E(\"non-existent\").count() %s != %s" % (i[0].count, 0))

    return errors


# tests an edge case where mongo aggregations fill fail to return a count when
# the ccollection doesnt exist
def test_count_when_no_data(O, man):
    errors = []

    i = list(O.query().V().count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.query().V().count()")
    elif i[0].count != 0:
        errors.append("Fail: O.query().V().count() %s != %s" % (i[0].count, 0))

    i = list(O.query().V("non-existent").count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.query().V(\"non-existent\").count()")
    elif i[0].count != 0:
        errors.append("Fail: O.query().V(\"non-existent\").count() %s != %s" % (i[0].count, 0))

    i = list(O.query().E().count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.query().E().count()")
    elif i[0].count != 0:
        errors.append("Fail: O.query().E().count() %s != %s" % (i[0].count, 0))

    i = list(O.query().E("non-existent").count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.query().E(\"non-existent\").count()")
    elif i[0].count != 0:
        errors.append("Fail: O.query().E(\"non-existent\").count() %s != %s" % (i[0].count, 0))

    return errors
