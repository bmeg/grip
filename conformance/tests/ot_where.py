from __future__ import absolute_import

import aql


def setupGraph(O):
    O.addVertex("vertex1", "person", {"name": "han", "age": 35, "occupation": "smuggler", "starships": ["millennium falcon"]})
    O.addVertex("vertex2", "person", {"name": "luke", "age": 26, "occupation": "jedi", "starships": ["x-wing", "millennium falcon"]})
    O.addVertex("vertex3", "robot", {"name": "r2-d2"})
    O.addVertex("vertex4", "robot", {"name": "c-3po"})
    O.addVertex("vertex5", "person", {"name": "obi-wan", "age": 63, "occupation": "jedi"})
    O.addVertex("vertex6", "person", {"name": "vader", "age": 55, "occupation": "sith", "starships": ["death star", "tie fighter"]})

    O.addEdge("vertex1", "vertex2", "friend", id="edge1")
    O.addEdge("vertex2", "vertex3", "owner", id="edge2")
    O.addEdge("vertex2", "vertex4", "friend", id="edge3")


def test_where_eq(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V().where(aql.eq("$.gid", "vertex3")).execute():
        count += 1
        if i['vertex']['gid'] != "vertex3":
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 1:
        errors.append(
            "Fail: O.query().V().where(aql.eq(\"$.gid\", \"vertex3\")) %s != %s" %
            (count, 1))

    count = 0
    for i in O.query().V().where(aql.eq("$.label", "person")).execute():
        count += 1
        if i['vertex']['label'] != "person":
            errors.append("Wrong vertex label %s" % (i['vertex']['label']))
    if count != 4:
        errors.append(
            "Fail: O.query().V().where(aql.eq(\"$.label\", \"person\")) %s != %s" %
            (count, 4))

    count = 0
    for i in O.query().V().where(aql.eq("$.occupation", "jedi")).execute():
        count += 1
        if i['vertex']['gid'] not in ["vertex2", "vertex5"]:
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 2:
        errors.append(
            "Fail: O.query().V().where(aql.eq(\"$.occupation\", \"jedi\")) %s != %s" %
            (count, 2))

    return errors


def test_where_neq(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V().where(aql.not_(aql.eq("$.gid", "vertex3"))).execute():
        count += 1
        if i['vertex']['gid'] == "vertex3":
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 5:
        errors.append(
            "Fail: O.query().V().where(aql.not_(aql.eq(\"$.gid\", \"vertex3\"))) %s != %s" %
            (count, 5))

    count = 0
    for i in O.query().V().where(aql.not_(aql.eq("$.label", "person"))).execute():
        count += 1
        if i['vertex']['label'] == "person":
            errors.append("Wrong vertex label %s" % (i['vertex']['label']))
    if count != 2:
        errors.append(
            "Fail: O.query().V().where(aql.not_(aql.eq(\"$.label\", \"person\"))) %s != %s" %
            (count, 2))

    count = 0
    for i in O.query().V().where(aql.not_(aql.eq("$.occupation", "jedi"))).execute():
        count += 1
        if i['vertex']['gid'] in ["vertex2", "vertex5"]:
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 4:
        errors.append(
            "Fail: O.query().V().where(aql.not_(aql.eq(\"$.occupation\", \"jedi\"))) %s != %s" %
            (count, 4))

    return errors


def test_where_in(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V().where((aql.in_("$.occupation", ["jedi", "sith"]))).execute():
        count += 1
        if i['vertex']['gid'] not in  ["vertex2", "vertex5", "vertex6"]:
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 3:
        errors.append(
            "Fail: O.query().V().where((aql.in_(\"$.occupation\", [\"jedi\", \"sith\"]))) %s != %s" %
            (count, 3))

    return errors


def test_where_contains(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V().where((aql.contains("$.starships", "x-wing"))).execute():
        count += 1
        if i['vertex']['gid'] not in  ["vertex2"]:
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 1:
        errors.append(
            "Fail: O.query().V().where((aql.in_(\"$.occupation\", [\"jedi\", \"sith\"]))) %s != %s" %
            (count, 1))

    return errors


def test_where_gt(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V().where(aql.gt("$.age", 35)).execute():
        count += 1
        if i['vertex']['gid'] not in ["vertex5", "vertex6"]:
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 2:
        errors.append(
            "Fail: O.query().V().where(aql.gt(\"$.age\", 35)) %s != %s" %
            (count, 2))

    count = 0
    for i in O.query().V().where(aql.gte("$.age", 35)).execute():
        count += 1
        if i['vertex']['gid'] not in ["vertex1", "vertex5", "vertex6"]:
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 3:
        errors.append(
            "Fail: O.query().V().where(aql.gte(\"$.age\", 35)) %s != %s" %
            (count, 3))

    return errors


def test_where_lt(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V().where(aql.lt("$.age", 35)).execute():
        count += 1
        if i['vertex']['gid'] not in ["vertex2"]:
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 1:
        errors.append(
            "Fail: O.query().V().where(aql.gt(\"$.age\", 35)) %s != %s" %
            (count, 1))

    count = 0
    for i in O.query().V().where(aql.lte("$.age", 35)).execute():
        count += 1
        if i['vertex']['gid'] not in ["vertex1", "vertex2",]:
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 2:
        errors.append(
            "Fail: O.query().V().where(aql.gte(\"$.age\", 35)) %s != %s" %
            (count, 2))

    return errors


def test_where_complex(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V().where(aql.and_(
            aql.eq("$.label", "person"),
            aql.not_(aql.or_(aql.eq("$.occupation", "jedi"),
                             aql.eq("$.occupation", "sith")))
    )).execute():
        count += 1
        if i['vertex']['gid'] != "vertex1":
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 1:
        errors.append(
            "Fail: O.query().V().where(aql.and_(aql.eq(\"$.label\", \"person\"), aql.not_(aql.eq(\"$.occupation\", \"jedi\")))) %s != %s" %
            (count, 1)
        )

    return errors
