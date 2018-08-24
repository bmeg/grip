from __future__ import absolute_import

import gripql


def setupGraph(O):
    O.addVertex("vertex1", "person", {"name": "han", "age": 35, "occupation": "smuggler", "starships": ["millennium falcon"]})
    O.addVertex("vertex2", "person", {"name": "luke", "age": 26, "occupation": "jedi", "starships": ["x-wing", "millennium falcon"]})
    O.addVertex("vertex3", "robot", {"name": "r2-d2"})
    O.addVertex("vertex4", "robot", {"name": "c-3po"})
    O.addVertex("vertex5", "person", {"name": "obi-wan", "age": 63, "occupation": "jedi"})
    O.addVertex("vertex6", "person", {"name": "vader", "age": 55, "occupation": "sith", "starships": ["death star", "tie fighter"]})

    O.addEdge("vertex1", "vertex2", "friend", gid="edge1")
    O.addEdge("vertex2", "vertex3", "owner", gid="edge2")
    O.addEdge("vertex2", "vertex4", "friend", gid="edge3")


def test_where_eq(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V().where(gripql.eq("_gid", "vertex3")):
        count += 1
        if i['gid'] != "vertex3":
            errors.append("Wrong vertex returned %s" % (i))
    if count != 1:
        errors.append(
            "Fail: O.query().V().where(gripql.eq(\"_gid\", \"vertex3\")) %s != %s" %
            (count, 1))

    count = 0
    for i in O.query().V().where(gripql.eq("_label", "person")):
        count += 1
        if i['label'] != "person":
            errors.append("Wrong vertex label %s" % (i['label']))
    if count != 4:
        errors.append(
            "Fail: O.query().V().where(gripql.eq(\"_label\", \"person\")) %s != %s" %
            (count, 4))

    count = 0
    for i in O.query().V().where(gripql.eq("occupation", "jedi")):
        count += 1
        if i['gid'] not in ["vertex2", "vertex5"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 2:
        errors.append(
            "Fail: O.query().V().where(gripql.eq(\"occupation\", \"jedi\")) %s != %s" %
            (count, 2))

    return errors


def test_where_neq(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V().where(gripql.neq("_gid", "vertex3")):
        count += 1
        if i['gid'] == "vertex3":
            errors.append("Wrong vertex returned %s" % (i))
    if count != 5:
        errors.append(
            "Fail: O.query().V().where(gripql.not_(gripql.eq(\"_gid\", \"vertex3\"))) %s != %s" %
            (count, 5))

    count = 0
    for i in O.query().V().where(gripql.neq("_label", "person")):
        count += 1
        if i['label'] == "person":
            errors.append("Wrong vertex label %s" % (i['label']))
    if count != 2:
        errors.append(
            "Fail: O.query().V().where(gripql.not_(gripql.eq(\"_label\", \"person\"))) %s != %s" %
            (count, 2))

    count = 0
    for i in O.query().V().where(gripql.neq("occupation", "jedi")):
        count += 1
        if i['gid'] in ["vertex2", "vertex5"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 4:
        errors.append(
            "Fail: O.query().V().where(gripql.not_(gripql.eq(\"occupation\", \"jedi\"))) %s != %s" %
            (count, 4))

    return errors


def test_where_in(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V().where(gripql.in_("occupation", ["jedi", "sith"])):
        count += 1
        if i['gid'] not in ["vertex2", "vertex5", "vertex6"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 3:
        errors.append(
            "Fail: O.query().V().where(gripql.in_(\"occupation\", [\"jedi\", \"sith\"])) %s != %s" %
            (count, 3))

    count = 0
    for i in O.query().V().where(gripql.in_("occupation", 0)):
        count += 1
    if count != 0:
        errors.append(
            "Fail: O.query().V().where(gripql.in_(\"occupation\", 0)) %s != %s" %
            (count, 0))

    return errors


def test_where_contains(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V().where(gripql.contains("starships", "x-wing")):
        count += 1
        if i['gid'] not in ["vertex2"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 1:
        errors.append(
            "Fail: O.query().V().where(gripql.contains(\"starships\", \"x-wing\")) %s != %s" %
            (count, 1))

    return errors


def test_where_gt(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V().where(gripql.gt("age", 35)):
        count += 1
        if i['gid'] not in ["vertex5", "vertex6"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 2:
        errors.append(
            "Fail: O.query().V().where(gripql.gt(\"age\", 35)) %s != %s" %
            (count, 2))

    count = 0
    for i in O.query().V().where(gripql.gte("age", 35)):
        count += 1
        if i['gid'] not in ["vertex1", "vertex5", "vertex6"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 3:
        errors.append(
            "Fail: O.query().V().where(gripql.gte(\"age\", 35)) %s != %s" %
            (count, 3))

    return errors


def test_where_lt(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V().where(gripql.lt("age", 35)):
        count += 1
        if i['gid'] not in ["vertex2"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 1:
        errors.append(
            "Fail: O.query().V().where(gripql.lt(\"age\", 35)) %s != %s" %
            (count, 1))

    count = 0
    for i in O.query().V().where(gripql.lte("age", 35)):
        count += 1
        if i['gid'] not in ["vertex1", "vertex2"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 2:
        errors.append(
            "Fail: O.query().V().where(gripql.lte(\"age\", 35)) %s != %s" %
            (count, 2))

    return errors


def test_where_and(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V().where(gripql.and_(gripql.eq("_label", "person"), gripql.eq("occupation", "jedi"))):
        count += 1
        if i['gid'] not in ["vertex2", "vertex5"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 2:
        errors.append(
            "Fail: O.query().V().where(gripql.and_(gripql.eq(\"_label\", \"person\"), gripql.eq(\"occupation\", \"jedi\"))) %s != %s" %
            (count, 2))

    return errors


def test_where_or(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V().where(gripql.or_(gripql.eq("occupation", "sith"), gripql.eq("occupation", "jedi"))):
        count += 1
        if i['gid'] not in ["vertex2", "vertex5", "vertex6"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 3:
        errors.append(
            "Fail: O.query().V().where(gripql.or_(gripql.eq(\"occupation\", \"sith\"), gripql.eq(\"occupation\", \"jedi\"))) %s != %s" %
            (count, 3))

    return errors


def test_where_not(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V().where(gripql.not_(gripql.eq("_label", "person"))):
        count += 1
        if i['gid'] not in ["vertex3", "vertex4"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 2:
        errors.append(
            "Fail: O.query().V().where(gripql.not_(gripql.eq(\"_label\", \"person\"))) %s != %s" %
            (count, 2))

    count = 0
    for i in O.query().V().where(gripql.not_(gripql.neq("_label", "person"))):
        count += 1
        if i['gid'] not in ["vertex1", "vertex2", "vertex5", "vertex6"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 4:
        errors.append(
            "Fail: O.query().V().where(gripql.not_(gripql.neq(\"_label\", \"person\"))) %s != %s" %
            (count, 4))

    return errors


def test_where_complex(O):
    errors = []
    setupGraph(O)

    count = 0
    for i in O.query().V().where(
            gripql.and_(
                gripql.eq("_label", "person"),
                gripql.not_(
                    gripql.or_(
                        gripql.eq("occupation", "jedi"),
                        gripql.eq("occupation", "sith")
                    )
                )
            )
    ):
        count += 1
        if i['gid'] != "vertex1":
            errors.append("Wrong vertex returned %s" % (i))
    if count != 1:
        errors.append(
            "Fail: O.query().V().where(gripql.and_(gripql.eq(\"_label\", \"person\"), gripql.not_(gripql.or_(gripql.eq(\"occupation\", \"jedi\"), gripql.eq(\"occupation\", \"sith\"))))) %s != %s" %
            (count, 1)
        )

    count = 0
    for i in O.query().V().where(
            gripql.not_(
                gripql.or_(
                    gripql.eq("_label", "robot"),
                    gripql.eq("occupation", "jedi"),
                )
            )
    ):
        count += 1
        if i['gid'] not in ["vertex1", "vertex6"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 2:
        errors.append(
            "Fail: O.query().V().where(gripql.not_(gripql.and_(gripql.eq(\"_label\", \"robot\"), gripql.eq(\"occupation\", \"jedi\")))) %s != %s" %
            (count, 2)
        )

    count = 0
    for i in O.query().V().where(
            gripql.not_(
                gripql.or_(
                    gripql.eq("_label", "robot"),
                    gripql.or_(
                        gripql.eq("occupation", "jedi"),
                        gripql.contains("starships", "millennium falcon")
                    )
                )
            )
    ):
        count += 1
        if i['gid'] != "vertex6":
            errors.append("Wrong vertex returned %s" % (i))
    if count != 1:
        errors.append(
            "Fail: O.query().V().where(gripql.not_(gripql.or_(gripql.eq(\"_label\", \"robot\"), gripql.or_(gripql.eq(\"occupation\", \"jedi\"),  gripql.contains(\"starships\", \"millennium falcon\"))))) %s != %s" %
            (count, 1)
        )

    count = 0
    for i in O.query().V().where(
            gripql.not_(
                gripql.and_(
                    gripql.eq("_label", "robot"),
                    gripql.or_(
                        gripql.eq("occupation", "jedi"),
                        gripql.contains("starships", "millennium falcon")
                    )
                )
            )
    ):
        count += 1
    if count != 6:
        errors.append(
            "Fail: O.query().V().where(gripql.not_(gripql.and_(gripql.eq(\"_label\", \"robot\"), gripql.or_(gripql.eq(\"occupation\", \"jedi\"),  gripql.contains(\"starships\", \"millennium falcon\"))))) %s != %s" %
            (count, 6)
        )

    return errors
