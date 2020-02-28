from __future__ import absolute_import

import re
import gripql


def test_hasLabel(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V().hasLabel("Robot"):
        count += 1
        if i['gid'] not in ["20", "21", "22"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 2:
        errors.append(
            "Fail: O.query().V().hasLabel(\"robot\") %s != %s" %
            (count, 2))

    for i in O.query().V().hasLabel("Starship"):
        if i['gid'] not in ["30", "31", "32", "33"]:
            errors.append("Wrong vertex returned %s" % (i))

    count = 0
    for i in O.query().V().hasLabel(["Robot", "Person"]):
        if "name" not in i.data:
            errors.append("vertex %s returned without data" % (i.gid))
        count += 1
    if count != 11:
        errors.append(
            "Fail: O.query().V().hasLabel([\"robot\", \"person\"]) %s != %s" %
            (count, 11))

    return errors


def test_hasKey(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V().hasKey("occupation"):
        count += 1
        if i['gid'] not in ["10", "11", "12", "13", "14"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 4:
        errors.append(
            "Fail: O.query().V().hasKey(\"occupation\") %s != %s" %
            (count, 4))

    count = 0
    for i in O.query().V().hasKey(["age", "starships"]):
        count += 1
        if i['gid'] not in ["10", "11", "13"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 3:
        errors.append(
            "Fail: O.query().V().hasKey([\"age\", \"starships\"]) %s != %s" %
            (count, 3))

    return errors


def test_hasId(O, man):
    errors = []

    man.setGraph("swapi")

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

    man.setGraph("swapi")

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
    if count != 9:
        errors.append(
            "Fail: O.query().V().has(gripql.eq(\"_label\", \"person\")) %s != %s" %
            (count, 9))

    count = 0
    for i in O.query().V().has(gripql.eq("occupation", "jedi")):
        count += 1
        if i['gid'] not in ["11", "12"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 2:
        errors.append(
            "Fail: O.query().V().has(gripql.eq(\"occupation\", \"jedi\")) %s != %s" %
            (count, 2))

    return errors


def test_has_neq(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V().has(gripql.neq("_gid", "03")):
        count += 1
        if i['gid'] == "03":
            errors.append("Wrong vertex returned %s" % (i))
    if count != 24:
        errors.append(
            "Fail: O.query().V().has(gripql.not_(gripql.eq(\"_gid\", \"03\"))) %s != %s" %
            (count, 24))

    count = 0
    for i in O.query().V().has(gripql.neq("_label", "Person")):
        count += 1
        if i['label'] == "Person":
            errors.append("Wrong vertex label %s" % (i['label']))
    if count != 16:
        errors.append(
            "Fail: O.query().V().has(gripql.not_(gripql.eq(\"_label\", \"person\"))) %s != %s" %
            (count, 16))

    count = 0
    for i in O.query().V().has(gripql.neq("occupation", "jedi")):
        count += 1
        if i['gid'] in ["15", "16"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 23:
        errors.append(
            "Fail: O.query().V().has(gripql.not_(gripql.eq(\"occupation\", \"jedi\"))) %s != %s" %
            (count, 23))

    return errors


def test_has_gt(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V().has(gripql.gt("age", 35)):
        count += 1
        if i['gid'] not in ["05", "07", "12", "13"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 4:
        errors.append(
            "Fail: O.query().V().has(gripql.gt(\"age\", 35)) %s != %s" %
            (count, 4))

    count = 0
    for i in O.query().V().has(gripql.gte("age", 35)):
        count += 1
        if i['gid'] not in ["03", "05", "06", "07", "10", "12", "13"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 6:
        errors.append(
            "Fail: O.query().V().has(gripql.gte(\"age\", 35)) %s != %s" %
            (count, 6))

    return errors


def test_has_lt(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V().has(gripql.lt("age", 35)):
        count += 1
        if i['gid'] not in ["01", "02", "03", "04", "06", "08", "09", "11"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 7:
        errors.append(
            "Fail: O.query().V().has(gripql.lt(\"age\", 35)) %s != %s" %
            (count, 7))

    count = 0
    for i in O.query().V().has(gripql.lte("age", 35)):
        count += 1
        if i['gid'] not in ["01", "02", "03", "04", "06", "08", "09", "10", "11"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 9:
        errors.append(
            "Fail: O.query().V().has(gripql.lte(\"age\", 35)) %s != %s" %
            (count, 9))

    return errors


def test_has_inside(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V().has(gripql.inside("age", 30, 60)):
        count += 1
        if i['gid'] not in ["03", "04", "05", "06", "07", "10", "13", "14", "17"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 6:
        errors.append(
            "Fail: O.query().V().has(gripql.inside(\"age\", 30, 60)) %s != %s" %
            (count, 6))

    return errors


def test_has_outside(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V().has(gripql.outside("age", 30, 60)):
        count += 1
        if i['gid'] not in ["01", "02", "08", "09", "11", "12", "15", "16"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 6:
        errors.append(
            "Fail: O.query().V().has(gripql.outside(\"age\", 30, 60)) %s != %s" %
            (count, 6))

    return errors


def test_has_between(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V().has(gripql.between("age", 26, 35)):
        count += 1
        if i['gid'] not in ["01", "04", "06", "08", "11"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 5:
        errors.append(
            "Fail: O.query().V().has(gripql.between(\"age\", 26, 35)) %s != %s" %
            (count, 5))

    return errors


def test_has_within(O, man):
    errors = []
    man.setGraph("swapi")

    count = 0
    for i in O.query().V().has(gripql.within("occupation", ["jedi", "sith"])):
        count += 1
        if i['gid'] not in ["11", "12", "13"]:
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

    man.setGraph("swapi")

    count = 0
    for i in O.query().V().has(gripql.without("occupation", ["jedi", "sith"])):
        count += 1
        if i['gid'] in ["11", "12", "13"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 22:
        errors.append(
            "Fail: O.query().V().has(gripql.without(\"occupation\", [\"jedi\", \"sith\"])) %s != %s" %
            (count, 22))

    count = 0
    for i in O.query().V().has(gripql.without("occupation", 0)):
        count += 1
    if count != 25:
        errors.append(
            "Fail: O.query().V().has(gripql.without(\"occupation\", 0)) %s != %s" %
            (count, 25))

    return errors


def test_has_contains(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V().has(gripql.contains("starships", "x-wing")):
        count += 1
        if i['gid'] not in ["11"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 1:
        errors.append(
            "Fail: O.query().V().has(gripql.contains(\"starships\", \"x-wing\")) %s != %s" %
            (count, 1))

    return errors


def test_has_and(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V().has(gripql.and_(gripql.eq("_label", "Character"), gripql.eq("occupation", "jedi"))):
        count += 1
        if i['gid'] not in ["11", "12"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 2:
        errors.append(
            "Fail: O.query().V().has(gripql.and_(gripql.eq(\"_label\", \"Character\"), gripql.eq(\"occupation\", \"jedi\"))) %s != %s" %
            (count, 2))

    return errors


def test_has_or(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V().has(gripql.or_(gripql.eq("occupation", "sith"), gripql.eq("occupation", "jedi"))):
        count += 1
        if i['gid'] not in ["11", "12", "13"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 3:
        errors.append(
            "Fail: O.query().V().has(gripql.or_(gripql.eq(\"occupation\", \"sith\"), gripql.eq(\"occupation\", \"jedi\"))) %s != %s" %
            (count, 3))

    return errors


def test_has_not(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V().has(gripql.not_(gripql.eq("_label", "Person"))):
        count += 1
        if re.search(r'^0\d$', i['gid']):
            errors.append("Wrong vertex returned %s" % (i))
    if count != 16:
        errors.append(
            "Fail: O.query().V().has(gripql.not_(gripql.eq(\"_label\", \"person\"))) %s != %s" %
            (count, 2))

    count = 0
    for i in O.query().V().has(gripql.not_(gripql.neq("_label", "Person"))):
        count += 1
        if not re.search(r'^0\d$', i['gid']):
            errors.append("Wrong vertex returned %s" % (i))
    if count != 9:
        errors.append(
            "Fail: O.query().V().has(gripql.not_(gripql.neq(\"_label\", \"person\"))) %s != %s" %
            (count, 9))

    return errors


def test_has_complex(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V().has(
            gripql.and_(
                gripql.eq("_label", "Character"),
                gripql.not_(
                    gripql.or_(
                        gripql.eq("occupation", "jedi"),
                        gripql.eq("occupation", "sith")
                    )
                )
            )
    ):
        count += 1
        if i['gid'] != "10":
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
                    gripql.eq("_label", "Robot"),
                    gripql.eq("occupation", "jedi"),
                )
            )
    ):
        count += 1
        if i['gid'] not in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "13", "30", "31", "32", "33", "40", "41", "42", "50", "51", "52"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 21:
        errors.append(
            "Fail: O.query().V().has(gripql.not_(gripql.and_(gripql.eq(\"_label\", \"robot\"), gripql.eq(\"occupation\", \"jedi\")))) %s != %s" %
            (count, 21)
        )

    count = 0
    for i in O.query().V().has(
            gripql.not_(
                gripql.or_(
                    gripql.eq("_label", "Robot"),
                    gripql.or_(
                        gripql.eq("occupation", "jedi"),
                        gripql.contains("starships", "millennium falcon")
                    )
                )
            )
    ):
        count += 1
        if i['gid'] not in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "13", "30", "31", "32", "33", "40", "41", "42", "50", "51", "52"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 20:
        errors.append(
            "Fail: O.query().V().has(gripql.not_(gripql.or_(gripql.eq(\"_label\", \"robot\"), gripql.or_(gripql.eq(\"occupation\", \"jedi\"),  gripql.contains(\"starships\", \"millennium falcon\"))))) %s != %s" %
            (count, 20)
        )

    count = 0
    for i in O.query().V().has(
            gripql.not_(
                gripql.and_(
                    gripql.eq("_label", "Robot"),
                    gripql.or_(
                        gripql.eq("occupation", "jedi"),
                        gripql.contains("starships", "millennium falcon")
                    )
                )
            )
    ):
        count += 1
    if count != 25:
        errors.append(
            "Fail: O.query().V().has(gripql.not_(gripql.and_(gripql.eq(\"_label\", \"robot\"), gripql.or_(gripql.eq(\"occupation\", \"jedi\"),  gripql.contains(\"starships\", \"millennium falcon\"))))) %s != %s" %
            (count, 25)
        )

    return errors
