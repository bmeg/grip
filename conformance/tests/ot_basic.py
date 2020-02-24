from __future__ import absolute_import

import re
import requests


def test_get_vertex(O, man):
    errors = []

    man.setGraph("graph1")

    expected = {
        u"gid": u"01",
        u"label": u"Person",
        u"data": {u"name": u"marko", u"age": 29}
    }

    try:
        resp = O.getVertex("01")
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


def test_get_edge(O, man):
    errors = []

    man.setGraph("graph1")

    expected = {
        u"gid": u"edge01-02",
        u"label": u"knows",
        u"from": u"01",
        u"to": u"02",
        u"data": {"weight": 0.5, "count": 20}
    }

    try:
        resp = O.getEdge("edge01-02")
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


def test_V(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for i in O.query().V():
        count += 1
        if re.search(r'^0\d$', i.gid ):
            if i.label != "Person":
                errors.append("Wrong vertex label. %s %s != %s" % (i.gid, i.label, "Person"))
        elif re.search(r'^1\d$', i.gid ):
            if i.label != "Character":
                errors.append("Wrong vertex label. %s != %s" % (i.label, "Character"))
        elif re.search(r'^2\d$', i.gid ):
            if i.label != "Robot":
                errors.append("Wrong vertex label. %s != %s" % (i.label, "Robot"))
        elif re.search(r'^3\d$', i.gid ):
            if i.label != "Starship":
                errors.append("Wrong vertex label. %s != %s" % (i.label, "Starship"))
        elif re.search(r'^4\d$', i.gid ):
            if i.label != "Movie":
                errors.append("Wrong vertex label. %s != %s" % (i.label, "Movie"))
        elif re.search(r'^5\d$', i.gid ):
            if i.label != "Book":
                errors.append("Wrong vertex label. %s != %s" % (i.label, "Book"))
        else:
            errors.append("Unknown vertex: %s" % (i.gid))

    if count != 25:
        errors.append("Fail: O.query().V() %s != %s" % (count, 25))

    count = 0
    for i in O.query().V("01"):
        if i['gid'] not in ["01"]:
            errors.append(
                "Fail: O.query().V(\"01\") - \
                Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 1:
        errors.append("Fail: O.query().V(\"1\") %s != %s" % (count, 1))

    return errors


def test_E(O, man):
    errors = []

    man.setGraph("graph1")
    #setupGraph(O)

    count = 0
    unknownCount = 0
    for i in O.query().E():
        count += 1
        if i.gid in ["edge01-02", "edge01-04"]:
            if i.label != "knows":
                errors.append("Wrong vertex label. %s != %s" % (i.label, "knows"))
        elif i.gid in ["edge3"]:
            if i.label != "parent":
                errors.append("Wrong vertex label. %s != %s" % (i.label, "parent"))
        else:
            unknownCount += 1

    if count == unknownCount:
        errors.append("Only found unnamed edges")

    if count != 13:
        errors.append("Fail: O.query().E() %s != %d" % (count, 13))

    count = 0
    for i in O.query().E("edge01-02"):
        if i['gid'] not in ["edge01-02"]:
            errors.append(
                "Fail: O.query().E(\"edge01-02\") - \
                Wrong edge %s" % (i['gid'])
            )
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().E(\"edge1\") %s != %d" % (count, 1))

    return errors


def test_outgoing(O, man):
    errors = []

    man.setGraph("graph1")
    #setupGraph(O)

    count = 0
    for i in O.query().V("02").out():
        if i['gid'] not in ["03", "05", "06"]:
            errors.append(
                "Fail: O.query().V(\"02\").out() - \
                Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 3:
        errors.append(
            "Fail: O.query().V(\"02\").out() %s != %d" % (count, 3))

    count = 0
    for i in O.query().V("02").out("friend"):
        if i['gid'] not in ["03", "06"]:
            errors.append(
                "Fail: O.query().V(\"02\").out(\"friend\") - \
                Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().V(\"vertex2\").out(\"friend\") %s != %d" % (count, 1))

    count = 0
    for i in O.query().E("edge01-02").out():
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().E(\"edge1\").out() %s != %d" % (count, 1))

    return errors


def test_incoming(O, man):
    errors = []

    man.setGraph("graph1")
    #setupGraph(O)

    count = 0
    for i in O.query().V("02").in_():
        if i['gid'] not in ["01"]:
            errors.append(
                "Fail: O.query().V(\"02\").in_() - \
                Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().V(\"02\").in_() %s != %d" % (count, 1))

    count = 0
    for i in O.query().V("02").in_("knows"):
        if i['gid'] not in ["01"]:
            errors.append(
                "Fail: O.query().V(\"02\").in_(\"knows\") - \
                Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().V(\"02\").in_(\"friend\") %s != %d" % (count, 1))

    count = 0
    for i in O.query().E("edge01-02").in_():
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().E(\"edge1\").in_() %s != %d" % (count, 1))

    return errors


def test_outgoing_edge(O, man):
    errors = []

    man.setGraph("graph1")
    #setupGraph(O)

    c = O.query().V("02").outE().count().execute()[0]["count"]
    if c != 3:
        errors.append("blank outgoing misscount: %d != %d" % (c, 3))

    for i in O.query().V("01").outE():
        if not i['gid'].startswith("edge01-"):
            errors.append("Wrong outgoing vertex %s" % (i['gid']))

    for i in O.query().V("02").outE().out():
        if i['gid'] not in ["03", "05", "06"]:
            errors.append("Wrong outgoing edge to vertex %s" % (i['gid']))

    if O.query().V("02").outE("friend").count().execute()[0]["count"] != 1:
        errors.append("labeled outgoing doesn't work")

    return errors


def test_incoming_edge(O, man):
    errors = []

    man.setGraph("graph1")
    #setupGraph(O)

    if O.query().V("02").inE().count().execute()[0]["count"] != 1:
        errors.append("blank incoming doesn't work")

    for i in O.query().V("04").inE():
        if i['gid'] not in ["edge01-04", "edge05-04"]:
            errors.append("Wrong incoming vertex %s" % (i['gid']))

    if list(O.query().V("03").inE("friend").count())[0]["count"] != 1:
        errors.append("labeled incoming doesn't work")

    return errors


def test_both(O, man):
    errors = []

    man.setGraph("graph1")
    #setupGraph(O)

    count = 0
    for row in O.query().V("01").both():
        count += 1
        if row['gid'] not in ["02", "03", "04", "08", "09", "10"]:
            errors.append("Fail: O.query().V(\"01\").both() - \
            Wrong vertex found: %s" % (row['gid']))
    if count != 6:
        errors.append("Fail: O.query().V(\"01\").both() %s != %s" % (count, 6))

    count = 0
    for row in O.query().V("02").both("parent"):
        count += 1
        if row['gid'] not in ["05"]:
            errors.append("Fail: O.query().V(\"02\").both(\"parent\") - \
            Wrong vertex found: %s" % (row['gid']))
    if count != 1:
        errors.append("Fail: O.query().V(\"vertex1\").both(\"parent\") %s != %s" % (count, 1))

    count = 0
    for row in O.query().E("edge01-02").both():
        count += 1
        if row['gid'] not in ["01", "02"]:
            errors.append("Fail: O.query().E(\"edge1\").both() - \
            Wrong vertex found: %s" % (row['gid']))
    if count != 2:
        errors.append("Fail: O.query().E(\"edge1\").both() %s != %s" % (count, 2))

    return errors


def test_both_edge(O, man):
    errors = []

    man.setGraph("graph1")
    #setupGraph(O)

    count = 0
    for row in O.query().V("05").bothE():
        count += 1
        if row['gid'] not in ["edge02-05", "edge05-42"]:
            errors.append("Fail: O.query().V(\"05\").bothE() - \
            Wrong edge found: %s" % (row['gid']))
    if count != 2:
        errors.append("Fail: O.query().V(\"vertex1\").bothE() %s != %s" % (count, 2))

    count = 0
    for row in O.query().V("02").bothE("parent"):
        count += 1
        if row['gid'] not in ["edge02-05"]:
            errors.append("Fail: O.query().V(\"02\").bothE(\"parent\") - \
            Wrong edge found: %s" % (row['gid']))
    if count != 1:
        errors.append("Fail: O.query().V(\"vertex1\").bothE(\"parent\") %s != %s" % (count, 1))

    return errors


def test_limit(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for row in O.query().V().limit(3):
        count += 1
        correct = ["01", "02", "03"]
        if row['gid'] not in correct:
            errors.append("Wrong vertex found: %s not in %s" % (row['gid'], correct))
    if count != 3:
        errors.append("Wrong vertex count found %s != %s" % (count, 3))

    count = 0
    for row in O.query().V("01").both().limit(3):
        count += 1
        correct = ["02", "03", "08"]
        if row['gid'] not in correct:
            errors.append("Wrong vertex found: %s not in %s" % (row['gid'], correct))
    if count != 3:
        errors.append("Wrong vertex count found %s != %s" % (count, 3))

    count = 0
    for row in O.query().V("01").bothE().limit(3):
        count += 1
        if row['gid'] not in ["edge08-01", "edge01-02", "edge01-03"]:
            errors.append("Wrong edge found: %s" % (row['gid']))
    if count != 3:
        errors.append("Wrong edge count found %s != %s" % (count, 3))

    return errors


def test_skip(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for row in O.query().V().skip(4).limit(2):
        count += 1
        if row['gid'] not in ["05", "06"]:
            errors.append("Wrong vertex found: %s" % (row['gid']))
    if count != 2:
        errors.append("Wrong vertex count found %s != %s" % (count, 2))

    count = 0
    for row in O.query().V("01").both().skip(4).limit(2):
        count += 1
        if row['gid'] not in ["09", "08"]:
            errors.append("""O.query().V("01").both().skip(4).limit(2): Wrong vertex found on both: %s""" % (row['gid']))
    if count != 2:
        errors.append("""O.query().V("01").both().skip(4).limit(2) : Wrong vertex count found %s != %s""" % (count, 2))

    count = 0
    for row in O.query().V("01").bothE().skip(4).limit(2):
        count += 1
        if row['gid'] not in ["edge01-09", "edge01-08"]:
            errors.append("Wrong edge found: %s" % (row['gid']))
    if count != 2:
        errors.append("Wrong edge count in bothE found %s != %s" % (count, 2))

    return errors


def test_range(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for row in O.query().V().range(4, 6):
        count += 1
        if row['gid'] not in ["05", "06"]:
            errors.append("Wrong vertex found: %s" % (row['gid']))
    if count != 2:
        errors.append("Wrong vertex count found %s != %s" % (count, 2))

    count = 0
    for row in O.query().V().range(4, -1):
        count += 1
    if count != 21:
        errors.append("Wrong vertex count found %s != %s" % (count, 21))

    return errors
