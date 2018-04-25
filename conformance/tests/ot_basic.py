from __future__ import absolute_import

import urllib2


def test_get_vertex(O):
    errors = []

    O.addVertex("vertex1", "person", {"name": "han", "occupation": "smuggler"})

    expected = {
        u"gid": u"vertex1",
        u"label": u"person",
        u"data": {u"name": u"han", u"occupation": u"smuggler"}
    }
    try:
        resp = O.getVertex("vertex1")
        if resp != expected:
            errors.append("Wrong vertex %s != %s" % (resp, expected))
    except Exception as e:
        errors.append("Unexpected error %s: %s" % (type(e).__name__, e))

    try:
        O.getVertex("i-dont-exist")
        errors.append("Expected 404")
    except urllib2.HTTPError as e:
        if e.code != 404:
            errors.append("Expected 404 not %s: %s" % (type(e).__name__, e))
    except Exception as e:
        errors.append("Expected 404 not %s: %s" % (type(e).__name__, e))

    return errors


def test_get_edge(O):
    errors = []

    O.addVertex("vertex1", "person", {"name": "han", "occupation": "smuggler"})
    O.addVertex("vertex2", "person", {"name": "luke", "occupation": "jedi"})

    O.addEdge("vertex1", "vertex2", "friend", id="edge1")

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
    except urllib2.HTTPError as e:
        if e.code != 404:
            errors.append("Expected 404 not %s: %s" % (type(e).__name__, e))
    except Exception as e:
        errors.append("Expected 404 not %s: %s" % (type(e).__name__, e))

    return errors


def test_has(O):
    errors = []

    O.addVertex("vertex1", "person", {"name": "han", "occupation": "smuggler"})
    O.addVertex("vertex2", "person", {"name": "luke", "occupation": "jedi"})
    O.addVertex("vertex3", "robot", {"name": "r2-d2"})
    O.addVertex("vertex4", "robot", {"name": "c-3po"})
    O.addVertex("vertex5", "person", {"name": "obi-wan", "occupation": "jedi"})

    O.addEdge("vertex1", "vertex2", "friend", id="edge1")
    O.addEdge("vertex2", "vertex3", "owner", id="edge2")
    O.addEdge("vertex2", "vertex4", "friend", id="edge3")

    count = 0
    for i in O.query().V().hasLabel("person").execute():
        count += 1
        if i['vertex']['label'] != "person":
            errors.append("Wrong vertex label %s" % (i['vertex']['label']))
    if count != 3:
        errors.append(
            "Fail: O.query().V().hasLabel('person') %s != %s" %
            (count, 3))

    count = 0
    for i in O.query().V().has("occupation", "jedi").execute():
        count += 1
        if i['vertex']['gid'] not in ["vertex2", "vertex5"]:
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 2:
        errors.append(
            "Fail: O.query().V().has('occupation', 'jedi') %s != %s" %
            (count, 2))

    count = 0
    for i in O.query().V().hasId("vertex3").execute():
        count += 1
        if i['vertex']['gid'] != "vertex3":
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 1:
        errors.append(
            "Fail: O.query().V().hasId('vertex3') %s != %s" %
            (count, 1))

    return errors


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
            "Fail: O.query().V('vertex1').outgoing().outgoing().has('field1', 'value4') %s != %s" %
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
