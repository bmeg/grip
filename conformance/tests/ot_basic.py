from __future__ import absolute_import

import aql
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
    for i in O.query().V("vertex1").out().execute():
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().V(\"vertex1\").out() %s != %d" % (count, 1))

    count = 0
    for i in O.query().V("vertex1").in_().execute():
        count += 1
    if count != 0:
        errors.append(
            "Fail: O.query().V(\"vertex1\").in_() %s != %d" % (count, 0))

    count = 0
    for i in O.query().V("vertex1").out().out().where(aql.eq("$.field2", "value4")).in_().execute():
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().V(\"vertex1\").out().out().where(aql.eq(\"$.field2\", \"value4\")).in_() %s != %s" %
            (count, 1))

    count = 0
    for i in O.query().E().execute():
        count += 1
    if count != 3:
        errors.append("Fail: O.query().E()")

    count = 0
    for i in O.query().E("edge1").execute():
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().E(\"edge1\") %s != %d" % (count, 1))

    count = 0
    for i in O.query().E("edge1").out().execute():
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().E(\"edge1\").out() %s != %d" % (count, 1))

    count = 0
    for i in O.query().E("edge1").in_().execute():
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().E(\"edge1\").in_() %s != %d" % (count, 1))

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

    if list(O.query().V("vertex2").out().count().execute())[0]["data"] != 2:
        errors.append("blank outgoing doesn't work")

    if list(O.query().V("vertex2").out("friend").count().execute())[0]["data"] != 1:
        errors.append("labeled outgoing doesn't work")

    for i in O.query().V("vertex2").out():
        if i['vertex']['gid'] not in ["vertex3", "vertex4"]:
            errors.append("Wrong outgoing vertex %s" % (i['vertex']['gid']))

    if list(O.query().V("vertex2").out("friend").count().execute())[0]["data"] != 1:
        errors.append("labeled outgoing doesn't work")

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

    if list(O.query().V("vertex2").in_().count().execute())[0]["data"] != 1:
        errors.append("blank incoming doesn't work")

    for i in O.query().V("vertex4").in_():
        if i['vertex']['gid'] not in ["vertex2"]:
            errors.append("Wrong incoming vertex %s" % (i['vertex']['gid']))

    if list(O.query().V("vertex3").in_("friend").count().execute())[0]["data"] != 1:
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

    if list(O.query().V("vertex2").outE().count().execute())[0]["data"] != 2:
        errors.append("blank outgoing doesn't work")

    for i in O.query().V("vertex2").outE():
        if i['edge']['gid'] not in ["edge1", "edge2"]:
            errors.append("Wrong outgoing vertex %s" % (i['edge']['gid']))

    if list(O.query().V("vertex2").outE("friend").count().execute())[0]["data"] != 1:
        errors.append("labeled outgoing doesn't work")

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

    if list(O.query().V("vertex2").inE().count().execute())[0]["data"] != 1:
        errors.append("blank incoming doesn't work")

    for i in O.query().V("vertex4").inE().execute():
        if i['edge']['gid'] not in ["edge2"]:
            errors.append("Wrong incoming vertex %s" % (i['edge']['gid']))

    if list(O.query().V("vertex3").inE("friend").count().execute())[0]["data"] != 1:
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


def test_both_edge(O):
    errors = []

    O.addVertex("vertex1", "person")
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person")
    O.addVertex("vertex4", "person")

    O.addEdge("vertex1", "vertex2", "friend", id="edge1")
    O.addEdge("vertex1", "vertex3", "friend", id="edge2")
    O.addEdge("vertex4", "vertex1", "parent", id="edge3")
    O.addEdge("vertex2", "vertex3", "parent", id="edge4")
    O.addEdge("vertex2", "vertex4", "friend", id="edge5")

    count = 0
    for row in O.query().V("vertex1").bothE().execute():
        count += 1
        if row['edge']['gid'] not in ["edge1", "edge2", "edge3"]:
            errors.append("Wrong edge found: %s" % (row['edge']['gid']))
    if count != 3:
        errors.append("Wrong edge count found %s != %s" % (count, 3))

    return errors
