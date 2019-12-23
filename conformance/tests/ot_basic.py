from __future__ import absolute_import

import requests


def setupGraph(O):
    O.addVertex("vertex1", "person", {"field1": "value1", "field2": "value2"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex4", "person")
    O.addVertex("vertex5", "dog")
    O.addVertex("vertex6", "dog")
    O.addVertex("vertex7", "dog")
    O.addVertex("vertex8", "dog")
    O.addVertex("vertex9", "car")
    O.addVertex("vertex10", "car")
    O.addVertex("vertex11", "car")
    O.addVertex("vertex12", "car")

    O.addEdge("vertex1", "vertex2", "friend", gid="edge1")
    O.addEdge("vertex2", "vertex3", "friend", gid="edge2")
    O.addEdge("vertex2", "vertex4", "parent", gid="edge3")


def test_get_vertex(O):
    errors = []
    setupGraph(O)

    expected = {
        u"gid": u"vertex1",
        u"label": u"person",
        u"data": {u"field1": u"value1", u"field2": u"value2"}
    }

    try:
        resp = O.getVertex("vertex1")
        if resp != expected:
            errors.append("Wrong vertex %s != %s" % (resp, expected))
    except Exception as e:
        errors.append("Unexpected error %s: %s" % (type(e).__name__, e))

    try:
        O.getVertex("i-dont-exist")
        errors.append("Expected HTTPError")
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            errors.append(
                "Expected 404 not %s: %s" % (e.response.status_code, e)
            )
    return errors


def test_get_edge(O):
    errors = []
    setupGraph(O)

    expected = {
        u"gid": u"edge1",
        u"label": u"friend",
        u"from": u"vertex1",
        u"to": u"vertex2",
        u"data": {}
    }

    try:
        resp = O.getEdge("edge1")
        if resp != expected:
            errors.append("Wrong edge %s != %s" % (resp, expected))
    except Exception as e:
        errors.append("Unexpected error %s: %s" % (type(e).__name__, e))

    try:
        O.getEdge("i-dont-exist")
        errors.append("Expected 404")
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            errors.append(
                "Expected 404 not %s: %s" % (e.response.status_code, e)
            )

    return errors


def test_V(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V():
        count += 1
        if i.gid in ["vertex1", "vertex2", "vertex3", "vertex4"]:
            if i.label != "person":
                errors.append("Wrong vertex label. %s != %s" % (i.label, "person"))
        elif i.gid in ["vertex5", "vertex6", "vertex7", "vertex8"]:
            if i.label != "dog":
                errors.append("Wrong vertex label. %s != %s" % (i.label, "dog"))
        elif i.gid in ["vertex9", "vertex10", "vertex11", "vertex12"]:
            if i.label != "car":
                errors.append("Wrong vertex label. %s != %s" % (i.label, "car"))
        else:
            errors.append("Unknown vertex: %s" % (i.gid))

    if count != 12:
        errors.append("Fail: O.query().V() %s != %s" % (count, 12))

    count = 0
    for i in O.query().V("vertex1"):
        if i['gid'] not in ["vertex1"]:
            errors.append(
                "Fail: O.query().V(\"vertex1\") - \
                Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 1:
        errors.append("Fail: O.query().V(\"vertex1\") %s != %s" % (count, 1))

    return errors


def test_E(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().E():
        count += 1
        if i.gid in ["edge1", "edge2"]:
            if i.label != "friend":
                errors.append("Wrong vertex label. %s != %s" % (i.label, "friend"))
        elif i.gid in ["edge3"]:
            if i.label != "parent":
                errors.append("Wrong vertex label. %s != %s" % (i.label, "parent"))
        else:
            errors.append("Unknown edge: %s" % (i.gid))

    if count != 3:
        errors.append("Fail: O.query().E() %s != %d" % (count, 3))

    count = 0
    for i in O.query().E("edge1"):
        if i['gid'] not in ["edge1"]:
            errors.append(
                "Fail: O.query().E(\"edge1\") - \
                Wrong edge %s" % (i['gid'])
            )
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().E(\"edge1\") %s != %d" % (count, 1))

    return errors


def test_outgoing(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V("vertex2").out():
        if i['gid'] not in ["vertex3", "vertex4"]:
            errors.append(
                "Fail: O.query().V(\"vertex2\").out() - \
                Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 2:
        errors.append(
            "Fail: O.query().V(\"vertex2\").out() %s != %d" % (count, 2))

    count = 0
    for i in O.query().V("vertex2").out("friend"):
        if i['gid'] not in ["vertex3"]:
            errors.append(
                "Fail: O.query().V(\"vertex2\").out(\"friend\") - \
                Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().V(\"vertex2\").out(\"friend\") %s != %d" % (count, 1))

    count = 0
    for i in O.query().E("edge1").out():
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().E(\"edge1\").out() %s != %d" % (count, 1))

    return errors


def test_incoming(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V("vertex2").in_():
        if i['gid'] not in ["vertex1"]:
            errors.append(
                "Fail: O.query().V(\"vertex2\").in_() - \
                Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().V(\"vertex2\").in_() %s != %d" % (count, 1))

    count = 0
    for i in O.query().V("vertex2").in_("friend"):
        if i['gid'] not in ["vertex1"]:
            errors.append(
                "Fail: O.query().V(\"vertex2\").in_(\"friend\") - \
                Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().V(\"vertex2\").in_(\"friend\") %s != %d" % (count, 1))

    count = 0
    for i in O.query().E("edge1").in_():
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().E(\"edge1\").in_() %s != %d" % (count, 1))

    return errors


def test_outgoing_edge(O):
    errors = []
    setupGraph(O)

    if O.query().V("vertex2").outE().count().execute()[0]["count"] != 2:
        errors.append("blank outgoing doesn't work")

    for i in O.query().V("vertex2").outE():
        if i['gid'] not in ["edge2", "edge3"]:
            errors.append("Wrong outgoing vertex %s" % (i['gid']))

    for i in O.query().V("vertex2").outE().out():
        if i['gid'] not in ["vertex3", "vertex4"]:
            errors.append("Wrong outgoing edge to vertex %s" % (i['gid']))

    if O.query().V("vertex2").outE("friend").count().execute()[0]["count"] != 1:
        errors.append("labeled outgoing doesn't work")

    return errors


def test_incoming_edge(O):
    errors = []
    setupGraph(O)

    if O.query().V("vertex2").inE().count().execute()[0]["count"] != 1:
        errors.append("blank incoming doesn't work")

    for i in O.query().V("vertex4").inE():
        if i['gid'] not in ["edge3"]:
            errors.append("Wrong incoming vertex %s" % (i['gid']))

    if list(O.query().V("vertex3").inE("friend").count())[0]["count"] != 1:
        errors.append("labeled incoming doesn't work")

    return errors


def test_both(O):
    errors = []
    setupGraph(O)

    count = 0
    for row in O.query().V("vertex1").both():
        count += 1
        if row['gid'] not in ["vertex2"]:
            errors.append("Fail: O.query().V(\"vertex1\").both() - \
            Wrong vertex found: %s" % (row['gid']))
    if count != 1:
        errors.append("Fail: O.query().V(\"vertex1\").both() %s != %s" % (count, 1))

    count = 0
    for row in O.query().V("vertex2").both("parent"):
        count += 1
        if row['gid'] not in ["vertex4"]:
            errors.append("Fail: O.query().V(\"vertex2\").both(\"parent\") - \
            Wrong vertex found: %s" % (row['gid']))
    if count != 1:
        errors.append("Fail: O.query().V(\"vertex1\").both(\"parent\") %s != %s" % (count, 1))

    count = 0
    for row in O.query().E("edge1").both():
        count += 1
        if row['gid'] not in ["vertex1", "vertex2"]:
            errors.append("Fail: O.query().E(\"edge1\").both() - \
            Wrong vertex found: %s" % (row['gid']))
    if count != 2:
        errors.append("Fail: O.query().E(\"edge1\").both() %s != %s" % (count, 2))

    return errors


def test_both_edge(O):
    errors = []
    setupGraph(O)

    count = 0
    for row in O.query().V("vertex1").bothE():
        count += 1
        if row['gid'] not in ["edge1"]:
            errors.append("Fail: O.query().V(\"vertex1\").bothE() - \
            Wrong edge found: %s" % (row['gid']))
    if count != 1:
        errors.append("Fail: O.query().V(\"vertex1\").bothE() %s != %s" % (count, 1))

    count = 0
    for row in O.query().V("vertex2").bothE("parent"):
        count += 1
        if row['gid'] not in ["edge3"]:
            errors.append("Fail: O.query().V(\"vertex2\").bothE(\"parent\") - \
            Wrong edge found: %s" % (row['gid']))
    if count != 1:
        errors.append("Fail: O.query().V(\"vertex1\").bothE(\"parent\") %s != %s" % (count, 1))

    return errors


def test_limit(O):
    errors = []
    O.addVertex("vertex1", "person")
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person")
    O.addVertex("vertex4", "person")
    O.addVertex("vertex5", "person")
    O.addVertex("vertex6", "person")
    O.addVertex("vertex7", "person")
    O.addVertex("vertex8", "person")
    O.addVertex("vertex9", "person")

    O.addEdge("vertex1", "vertex2", "friend", gid="edge1")
    O.addEdge("vertex1", "vertex3", "friend", gid="edge2")
    O.addEdge("vertex1", "vertex7", "parent", gid="edge3")
    O.addEdge("vertex1", "vertex9", "parent", gid="edge4")
    O.addEdge("vertex2", "vertex1", "enemy", gid="edge5")
    O.addEdge("vertex8", "vertex1", "enemy", gid="edge6")

    count = 0
    for row in O.query().V().limit(3):
        count += 1
        correct = ["vertex1", "vertex2", "vertex3"]
        if row['gid'] not in correct:
            errors.append("Wrong vertex found: %s not in %s" % (row['gid'], correct))
    if count != 3:
        errors.append("Wrong vertex count found %s != %s" % (count, 3))

    count = 0
    for row in O.query().V("vertex1").both().limit(3):
        count += 1
        correct = ["vertex2", "vertex8", "vertex2"]
        if row['gid'] not in correct:
            errors.append("Wrong vertex found: %s not in %s" % (row['gid'], correct))
    if count != 3:
        errors.append("Wrong vertex count found %s != %s" % (count, 3))

    count = 0
    for row in O.query().V("vertex1").bothE().limit(3):
        count += 1
        if row['gid'] not in ["edge5", "edge6", "edge1"]:
            errors.append("Wrong edge found: %s" % (row['gid']))
    if count != 3:
        errors.append("Wrong edge count found %s != %s" % (count, 3))

    return errors


def test_skip(O):
    errors = []

    O.addVertex("vertex1", "person")
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person")
    O.addVertex("vertex4", "person")
    O.addVertex("vertex5", "person")
    O.addVertex("vertex6", "person")
    O.addVertex("vertex7", "person")
    O.addVertex("vertex8", "person")
    O.addVertex("vertex9", "person")

    O.addEdge("vertex1", "vertex2", "friend", gid="edge1")
    O.addEdge("vertex1", "vertex3", "friend", gid="edge2")
    O.addEdge("vertex1", "vertex7", "parent", gid="edge3")
    O.addEdge("vertex1", "vertex9", "parent", gid="edge4")
    O.addEdge("vertex2", "vertex1", "enemy", gid="edge5")
    O.addEdge("vertex8", "vertex1", "enemy", gid="edge6")

    count = 0
    for row in O.query().V().skip(4).limit(2):
        count += 1
        if row['gid'] not in ["vertex5", "vertex6"]:
            errors.append("Wrong vertex found: %s" % (row['gid']))
    if count != 2:
        errors.append("Wrong vertex count found %s != %s" % (count, 2))

    count = 0
    for row in O.query().V("vertex1").both().skip(4).limit(2):
        count += 1
        if row['gid'] not in ["vertex7", "vertex9"]:
            errors.append("Wrong vertex found: %s" % (row['gid']))
    if count != 2:
        errors.append("Wrong vertex count found %s != %s" % (count, 2))

    count = 0
    for row in O.query().V("vertex1").bothE().skip(4).limit(2):
        count += 1
        if row['gid'] not in ["edge3", "edge4"]:
            errors.append("Wrong edge found: %s" % (row['gid']))
    if count != 2:
        errors.append("Wrong edge count found %s != %s" % (count, 2))

    return errors


def test_range(O):
    errors = []

    O.addVertex("vertex1", "person")
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person")
    O.addVertex("vertex4", "person")
    O.addVertex("vertex5", "person")
    O.addVertex("vertex6", "person")
    O.addVertex("vertex7", "person")
    O.addVertex("vertex8", "person")
    O.addVertex("vertex9", "person")

    O.addEdge("vertex1", "vertex2", "friend", gid="edge1")
    O.addEdge("vertex1", "vertex3", "friend", gid="edge2")
    O.addEdge("vertex1", "vertex7", "parent", gid="edge3")
    O.addEdge("vertex1", "vertex9", "parent", gid="edge4")
    O.addEdge("vertex2", "vertex1", "enemy", gid="edge5")
    O.addEdge("vertex8", "vertex1", "enemy", gid="edge6")

    count = 0
    for row in O.query().V().range(4, 6):
        count += 1
        if row['gid'] not in ["vertex5", "vertex6"]:
            errors.append("Wrong vertex found: %s" % (row['gid']))
    if count != 2:
        errors.append("Wrong vertex count found %s != %s" % (count, 2))

    count = 0
    for row in O.query().V().range(4, -1):
        count += 1
    if count != 5:
        errors.append("Wrong vertex count found %s != %s" % (count, 5))

    return errors
