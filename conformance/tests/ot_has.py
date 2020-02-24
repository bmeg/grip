from __future__ import absolute_import

import gripql


def test_hasLabel(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for i in O.query().V().hasLabel("Robot"):
        count += 1
        if i['gid'] not in ["40", "41"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 2:
        errors.append(
            "Fail: O.query().V().hasLabel(\"robot\") %s != %s" %
            (count, 2))

    for i in O.query().V().hasLabel("Car"):
        if i['gid'] not in ["30", "31", "32", "33"]:
            errors.append("Wrong vertex returned %s" % (i))

    count = 0
    for i in O.query().V().hasLabel(["Robot", "Person"]):
        if "name" not in i.data:
            errors.append("vertex %s returned without data" % (i.gid))
        count += 1
    if count != 16:
        errors.append(
            "Fail: O.query().V().hasLabel([\"robot\", \"person\"]) %s != %s" %
            (count, 16))

    return errors


def test_hasKey(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for i in O.query().V().hasKey("occupation"):
        count += 1
        if i['gid'] not in ["14", "15", "16", "17"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 4:
        errors.append(
            "Fail: O.query().V().hasKey(\"occupation\") %s != %s" %
            (count, 4))

    count = 0
    for i in O.query().V().hasKey(["age", "starships"]):
        count += 1
        if i['gid'] not in ["14", "15", "17"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 3:
        errors.append(
            "Fail: O.query().V().hasKey([\"age\", \"starships\"]) %s != %s" %
            (count, 3))

    return errors


def test_hasId(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for i in O.query().V().hasId("01"):
        count += 1
        if i['gid'] != "01":
            errors.append("Wrong vertex returned %s" % (i))
    if count != 1:
        errors.append(
            "Fail: O.query().V().hasId(\"01\") %s != %s" %
            (count, 1))

    count = 0
    for i in O.query().V().hasId(["01", "02"]):
        count += 1
        if i['gid'] not in ["01", "02"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 2:
        errors.append(
            "Fail: O.query().V().hasId([\"01\", \"02\"]) %s != %s" %
            (count, 2))

    return errors


def test_has_eq(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for i in O.query().V().has(gripql.eq("_gid", "03")):
        count += 1
        if i['gid'] != "03":
            errors.append("Wrong vertex returned %s" % (i))
    if count != 1:
        errors.append(
            "Fail: O.query().V().has(gripql.eq(\"_gid\", \"03\")) %s != %s" %
            (count, 1))

    count = 0
    for i in O.query().V().has(gripql.eq("_label", "Person")):
        count += 1
        if i['label'] != "Person":
            errors.append("Wrong vertex label %s" % (i['label']))
    if count != 14:
        errors.append(
            "Fail: O.query().V().has(gripql.eq(\"_label\", \"person\")) %s != %s" %
            (count, 14))

    count = 0
    for i in O.query().V().has(gripql.eq("occupation", "jedi")):
        count += 1
        if i['gid'] not in ["15", "16"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 2:
        errors.append(
            "Fail: O.query().V().has(gripql.eq(\"occupation\", \"jedi\")) %s != %s" %
            (count, 2))

    return errors


def test_has_neq(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for i in O.query().V().has(gripql.neq("_gid", "03")):
        count += 1
        if i['gid'] == "03":
            errors.append("Wrong vertex returned %s" % (i))
    if count != 26:
        errors.append(
            "Fail: O.query().V().has(gripql.not_(gripql.eq(\"_gid\", \"03\"))) %s != %s" %
            (count, 26))

    count = 0
    for i in O.query().V().has(gripql.neq("_label", "Person")):
        count += 1
        if i['label'] == "Person":
            errors.append("Wrong vertex label %s" % (i['label']))
    if count != 13:
        errors.append(
            "Fail: O.query().V().has(gripql.not_(gripql.eq(\"_label\", \"person\"))) %s != %s" %
            (count, 13))

    count = 0
    for i in O.query().V().has(gripql.neq("occupation", "jedi")):
        count += 1
        if i['gid'] in ["15", "16"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 25:
        errors.append(
            "Fail: O.query().V().has(gripql.not_(gripql.eq(\"occupation\", \"jedi\"))) %s != %s" %
            (count, 25))

    return errors


def test_has_gt(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for i in O.query().V().has(gripql.gt("age", 35)):
        count += 1
        if i['gid'] not in ["07", "10", "13", "16", "17"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 5:
        errors.append(
            "Fail: O.query().V().has(gripql.gt(\"age\", 35)) %s != %s" %
            (count, 5))

    count = 0
    for i in O.query().V().has(gripql.gte("age", 35)):
        count += 1
        if i['gid'] not in ["06", "07", "10", "13", "14", "16", "17"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 7:
        errors.append(
            "Fail: O.query().V().has(gripql.gte(\"age\", 35)) %s != %s" %
            (count, 7))

    return errors


def test_has_lt(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for i in O.query().V().has(gripql.lt("age", 35)):
        count += 1
        if i['gid'] not in ["01", "02", "04", "09", "11", "12", "15"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 7:
        errors.append(
            "Fail: O.query().V().has(gripql.lt(\"age\", 35)) %s != %s" %
            (count, 7))

    count = 0
    for i in O.query().V().has(gripql.lte("age", 35)):
        count += 1
        if i['gid'] not in ["01", "02", "04", "06", "09", "11", "12", "14", "15"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 9:
        errors.append(
            "Fail: O.query().V().has(gripql.lte(\"age\", 35)) %s != %s" %
            (count, 9))

    return errors


def test_has_inside(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for i in O.query().V().has(gripql.inside("age", 30, 60)):
        count += 1
        if i['gid'] not in ["04", "06", "07", "10", "13", "14", "17"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 7:
        errors.append(
            "Fail: O.query().V().has(gripql.inside(\"age\", 30, 60)) %s != %s" %
            (count, 7))

    return errors


def test_has_outside(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for i in O.query().V().has(gripql.outside("age", 30, 60)):
        count += 1
        if i['gid'] not in ["01", "02", "11", "12", "15", "16"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 6:
        errors.append(
            "Fail: O.query().V().has(gripql.outside(\"age\", 30, 60)) %s != %s" %
            (count, 6))

    return errors


def test_has_between(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for i in O.query().V().has(gripql.between("age", 26, 35)):
        count += 1
        if i['gid'] not in ["vertex2"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 1:
        errors.append(
            "Fail: O.query().V().has(gripql.between(\"age\", 26, 35)) %s != %s" %
            (count, 1))

    return errors


def test_has_within(O, man):
    errors = []
    man.setGraph("graph1")

    count = 0
    for i in O.query().V().has(gripql.within("occupation", ["jedi", "sith"])):
        count += 1
        if i['gid'] not in ["vertex2", "vertex5", "vertex6"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 3:
        errors.append(
            "Fail: O.query().V().has(gripql.within(\"occupation\", [\"jedi\", \"sith\"])) %s != %s" %
            (count, 3))

    count = 0
    for i in O.query().V().has(gripql.within("occupation", 0)):
        count += 1
    if count != 0:
        errors.append(
            "Fail: O.query().V().has(gripql.within(\"occupation\", 0)) %s != %s" %
            (count, 0))

    return errors


def test_has_without(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for i in O.query().V().has(gripql.without("occupation", ["jedi", "sith"])):
        count += 1
        if i['gid'] not in ["vertex1", "vertex3", "vertex4"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 3:
        errors.append(
            "Fail: O.query().V().has(gripql.without(\"occupation\", [\"jedi\", \"sith\"])) %s != %s" %
            (count, 3))

    count = 0
    for i in O.query().V().has(gripql.without("occupation", 0)):
        count += 1
    if count != 6:
        errors.append(
            "Fail: O.query().V().has(gripql.without(\"occupation\", 0)) %s != %s" %
            (count, 6))

    return errors


def test_has_contains(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for i in O.query().V().has(gripql.contains("starships", "x-wing")):
        count += 1
        if i['gid'] not in ["vertex2"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 1:
        errors.append(
            "Fail: O.query().V().has(gripql.contains(\"starships\", \"x-wing\")) %s != %s" %
            (count, 1))

    return errors


def test_has_and(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for i in O.query().V().has(gripql.and_(gripql.eq("_label", "person"), gripql.eq("occupation", "jedi"))):
        count += 1
        if i['gid'] not in ["vertex2", "vertex5"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 2:
        errors.append(
            "Fail: O.query().V().has(gripql.and_(gripql.eq(\"_label\", \"person\"), gripql.eq(\"occupation\", \"jedi\"))) %s != %s" %
            (count, 2))

    return errors


def test_has_or(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for i in O.query().V().has(gripql.or_(gripql.eq("occupation", "sith"), gripql.eq("occupation", "jedi"))):
        count += 1
        if i['gid'] not in ["vertex2", "vertex5", "vertex6"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 3:
        errors.append(
            "Fail: O.query().V().has(gripql.or_(gripql.eq(\"occupation\", \"sith\"), gripql.eq(\"occupation\", \"jedi\"))) %s != %s" %
            (count, 3))

    return errors


def test_has_not(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for i in O.query().V().has(gripql.not_(gripql.eq("_label", "person"))):
        count += 1
        if i['gid'] not in ["vertex3", "vertex4"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 2:
        errors.append(
            "Fail: O.query().V().has(gripql.not_(gripql.eq(\"_label\", \"person\"))) %s != %s" %
            (count, 2))

    count = 0
    for i in O.query().V().has(gripql.not_(gripql.neq("_label", "person"))):
        count += 1
        if i['gid'] not in ["vertex1", "vertex2", "vertex5", "vertex6"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 4:
        errors.append(
            "Fail: O.query().V().has(gripql.not_(gripql.neq(\"_label\", \"person\"))) %s != %s" %
            (count, 4))

    return errors


def test_has_complex(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for i in O.query().V().has(
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
            "Fail: O.query().V().has(gripql.and_(gripql.eq(\"_label\", \"person\"), gripql.not_(gripql.or_(gripql.eq(\"occupation\", \"jedi\"), gripql.eq(\"occupation\", \"sith\"))))) %s != %s" %
            (count, 1)
        )

    count = 0
    for i in O.query().V().has(
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
            "Fail: O.query().V().has(gripql.not_(gripql.and_(gripql.eq(\"_label\", \"robot\"), gripql.eq(\"occupation\", \"jedi\")))) %s != %s" %
            (count, 2)
        )

    count = 0
    for i in O.query().V().has(
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
            "Fail: O.query().V().has(gripql.not_(gripql.or_(gripql.eq(\"_label\", \"robot\"), gripql.or_(gripql.eq(\"occupation\", \"jedi\"),  gripql.contains(\"starships\", \"millennium falcon\"))))) %s != %s" %
            (count, 1)
        )

    count = 0
    for i in O.query().V().has(
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
            "Fail: O.query().V().has(gripql.not_(gripql.and_(gripql.eq(\"_label\", \"robot\"), gripql.or_(gripql.eq(\"occupation\", \"jedi\"),  gripql.contains(\"starships\", \"millennium falcon\"))))) %s != %s" %
            (count, 6)
        )

    return errors
