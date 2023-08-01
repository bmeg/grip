from __future__ import absolute_import

import gripql


def test_hasLabel(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().hasLabel("Vehicle"):
        count += 1
        if not i['gid'].startswith("Vehicle:"):
            errors.append("Wrong vertex returned %s" % (i))
    if count != 4:
        errors.append(
            "Fail: G.query().V().hasLabel(\"Vehicle\") %s != %s" %
            (count, 4))

    for i in G.query().V().hasLabel("Starship"):
        if not i['gid'].startswith("Starship:"):
            errors.append("Wrong vertex returned %s" % (i))

    count = 0
    for i in G.query().V().hasLabel(["Vehicle", "Starship"]):
        if "name" not in i["data"]:
            errors.append("vertex %s returned without data" % (i.gid))
        count += 1
    if count != 12:
        errors.append(
            "Fail: G.query().V().hasLabel([\"Vehicle\", \"Starship\"]) %s != %s" %
            (count, 12))

    return errors


def test_hasKey(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().hasKey("manufacturer"):
        count += 1
        if not i['gid'].startswith("Vehicle:") and not i['gid'].startswith("Starship:"):
            errors.append("Wrong vertex returned %s" % (i))
    if count != 12:
        errors.append(
            "Fail: G.query().V().hasKey(\"manufacturer\") %s != %s" %
            (count, 12))

    count = 0
    for i in G.query().V().hasKey(["hyperdrive_rating", "manufacturer"]):
        count += 1
        if not i['gid'].startswith("Starship:"):
            errors.append("Wrong vertex returned %s" % (i))
    if count != 8:
        errors.append(
            "Fail: G.query().V().hasKey([\"hyperdrive_rating\", \"manufacturer\"]) %s != %s" %
            (count, 8))

    return errors


def test_hasId(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().hasId("Character:1"):
        count += 1
        if i['gid'] != "Character:1":
            errors.append("Wrong vertex returned %s" % (i))
    if count != 1:
        errors.append(
            "Fail: G.query().V().hasId(\"01\") %s != %s" %
            (count, 1))

    count = 0
    for i in G.query().V().hasId(["Character:1", "Character:2"]):
        count += 1
        if i['gid'] not in ["Character:1", "Character:2"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 2:
        errors.append(
            "Fail: G.query().V().hasId([\"Character:1\", \"Character:2\"]) %s != %s" %
            (count, 2))

    return errors


def test_has_eq(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().has(gripql.eq("_gid", "Character:3")):
        count += 1
        if i['gid'] != "Character:3":
            errors.append("Wrong vertex returned %s" % (i))
    if count != 1:
        errors.append(
            "Fail: G.query().V().has(gripql.eq(\"_gid\", \"Character:3\")) %s != %s" %
            (count, 1))

    count = 0
    for i in G.query().V().has(gripql.eq("_label", "Character")):
        count += 1
        if i['label'] != "Character":
            errors.append("Wrong vertex label %s" % (i['label']))
    if count != 18:
        errors.append(
            "Fail: G.query().V().has(gripql.eq(\"_label\", \"person\")) %s != %s" %
            (count, 18))

    count = 0
    for i in G.query().V().has(gripql.eq("eye_color", "brown")):
        count += 1
        if i['gid'] not in ["Character:14", "Character:5", "Character:81", "Character:9"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 4:
        errors.append(
            "Fail: G.query().V().has(gripql.eq(\"eye_color\", \"brown\")) %s != %s" %
            (count, 4))

    return errors


def test_has_prev(man):
    errors = []
    G = man.setGraph("swapi")

    q = G.query().V().hasLabel("Character").as_("1").out("homeworld").out("residents")
    q = q.has(gripql.neq("$1._gid", "$._gid"))
    for i in q.render(["$1._gid", "$._gid"]):
        if i[0] == i[1]:
            errors.append("History based filter failed: %s" % (i[0]) )
    return errors


def test_has_neq(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().has(gripql.neq("_gid", "Character:1")):
        count += 1
        if i['gid'] == "Character:1":
            errors.append("Wrong vertex returned %s" % (i))
    if count != 38:
        errors.append(
            "Fail: G.query().V().has(gripql.not_(gripql.eq(\"_gid\", \"Character:1\"))) %s != %s" %
            (count, 38))

    count = 0
    for i in G.query().V().has(gripql.neq("_label", "Character")):
        count += 1
        if i['label'] == "Character":
            errors.append("Wrong vertex label %s" % (i['label']))
    if count != 21:
        errors.append(
            "Fail: G.query().V().has(gripql.not_(gripql.eq(\"_label\", \"Character\"))) %s != %s" %
            (count, 21))

    count = 0
    for i in G.query().V().hasLabel("Character").has(gripql.neq("eye_color", "brown")):
        count += 1
        if i['data']["eye_color"] == "brown":
            errors.append("Wrong vertex returned %s" % (i))
    if count != 14:
        errors.append(
            "Fail: G.query().V().has(gripql.not_(gripql.eq(\"eye_color\", \"brown\"))) %s != %s" %
            (count, 14))

    return errors


def test_has_gt(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().has(gripql.gt("height", 202)):
        count += 1
        if i['gid'] not in ["Character:13"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 1:
        errors.append(
            "Fail: G.query().V().has(gripql.gt(\"height\", 200)) %s != %s" %
            (count, 1))

    count = 0
    for i in G.query().V().has(gripql.gte("height", 202)):
        count += 1
        if i['gid'] not in ["Character:4", "Character:13"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 2:
        errors.append(
            "Fail: G.query().V().has(gripql.gte(\"height\", 202)) %s != %s" %
            (count, 2))

    return errors


def test_has_lt(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().has(gripql.lt("height", 97)):
        count += 1
        if i['gid'] not in ["Character:3"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 1:
        errors.append(
            "Fail: G.query().V().has(gripql.lt(\"height\", 97)) %s != %s" %
            (count, 1))

    count = 0
    for i in G.query().V().has(gripql.lte("height", 97)):
        count += 1
        if i['gid'] not in ["Character:3", "Character:8"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 2:
        errors.append(
            "Fail: G.query().V().has(gripql.lte(\"height\", 97)) %s != %s" %
            (count, 2))

    return errors


def test_has_inside(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().has(gripql.inside("height", 100, 200)):
        count += 1
        if i['gid'] in ["Character:3", "Character:4", "Character:8", "Character:13"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 14:
        errors.append(
            "Fail: G.query().V().has(gripql.inside(\"age\", 30, 60)) %s != %s" %
            (count, 14))

    return errors


def test_has_outside(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().has(gripql.outside("height", 100, 200)):
        count += 1
        if i['gid'] not in ["Character:3", "Character:4", "Character:8", "Character:13"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 4:
        errors.append(
            "Fail: G.query().V().has(gripql.outside(\"age\", 30, 60)) %s != %s" %
            (count, 4))

    return errors


def test_has_between(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().has(gripql.between("height", 180, 200)):
        count += 1
        if i['gid'] not in ["Character:10", "Character:12", "Character:14", "Character:19", "Character:81", "Character:9"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 6:
        errors.append(
            "Fail: G.query().V().has(gripql.between(\"height\", 180, 200)) %s != %s" %
            (count, 5))

    return errors


def test_has_within(man):
    errors = []
    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().has(gripql.within("eye_color", ["brown", "hazel"])):
        count += 1
        if i['gid'] not in ["Character:14", "Character:18", "Character:5", "Character:81", "Character:9"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 5:
        errors.append(
            "Fail: G.query().V().has(gripql.within(\"eye_color\", [\"brown\", \"hazel\"])) %s != %s" %
            (count, 5))

    count = 0
    for i in G.query().V().has(gripql.within("eye_color", 0)):
        count += 1
    if count != 0:
        errors.append(
            "Fail: G.query().V().has(gripql.within(\"eye_color\", 0)) %s != %s" %
            (count, 0))

    return errors


def test_has_without(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().has(gripql.without("eye_color", ["brown"])):
        count += 1
        if i['gid'] in ["Character:5", "Character:9", "Character:14", "Character:81"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 35:
        errors.append(
            "Fail: G.query().V().has(gripql.without(\"occupation\", [\"jedi\", \"sith\"])) %s != %s" %
            (count, 35))

    count = 0
    for i in G.query().V().has(gripql.without("occupation", 0)):
        count += 1
    if count != 39:
        errors.append(
            "Fail: G.query().V().has(gripql.without(\"occupation\", 0)) %s != %s" %
            (count, 39))

    return errors


def test_has_contains(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().has(gripql.contains("terrain", "jungle")):
        count += 1
        if i['gid'] not in ["Planet:3"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 1:
        errors.append(
            "Fail: G.query().V().has(gripql.contains(\"terrain\", \"jungle\")) %s != %s" %
            (count, 1))

    return errors


def test_has_and(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().has(gripql.and_(gripql.eq("_label", "Character"), gripql.eq("eye_color", "blue"))):
        count += 1
        if i['gid'] not in ["Character:1", "Character:12", "Character:13", "Character:19", "Character:6", "Character:7"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 6:
        errors.append(
            "Fail: G.query().V().has(gripql.and_(gripql.eq(\"_label\", \"Character\"), gripql.eq(\"eye_color\", \"blue\"))) %s != %s" %
            (count, 6))

    return errors


def test_has_or(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().has(gripql.or_(gripql.eq("eye_color", "blue"), gripql.eq("eye_color", "hazel"))):
        count += 1
        if i['gid'] not in ["Character:1", "Character:12", "Character:13", "Character:18", "Character:19", "Character:6", "Character:7"]:
            errors.append("Wrong vertex returned %s" % (i))
    if count != 7:
        errors.append(
            "Fail: G.query().V().has(gripql.or_(gripql.eq(\"eye_color\", \"blue\"), gripql.eq(\"eye_color\", \"hazel\"))) %s != %s" %
            (count, 7))

    return errors


def test_has_not(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().has(gripql.not_(gripql.eq("_label", "Character"))):
        count += 1
        if i['gid'].startswith("Character"):
            errors.append("Wrong vertex returned %s" % (i))
    if count != 21:
        errors.append(
            "Fail: G.query().V().has(gripql.not_(gripql.eq(\"_label\", \"Character\"))) %s != %s" %
            (count, 21))

    count = 0
    for i in G.query().V().has(gripql.not_(gripql.neq("_label", "Character"))):
        count += 1
        if not i['gid'].startswith("Character"):
            errors.append("Wrong vertex returned %s" % (i))
    if count != 18:
        errors.append(
            "Fail: G.query().V().has(gripql.not_(gripql.neq(\"_label\", \"Character\"))) %s != %s" %
            (count, 18))

    return errors


def test_has_complex(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().has(
            gripql.and_(
                gripql.eq("_label", "Character"),
                gripql.not_(
                    gripql.or_(
                        gripql.eq("eye_color", "brown"),
                        gripql.eq("eye_color", "hazel")
                    )
                )
            )
    ):
        count += 1
        if i['label'] == "Character" and (i["data"]["eye_color"] == "brown" or i["data"]["eye_color"] == "hazel"):
            errors.append("Wrong vertex returned %s" % (i))
    if count != 13:
        errors.append(
            "Fail: G.query().V().has(gripql.and_(gripql.eq(\"_label\", \"Character\"), gripql.not_(gripql.or_(gripql.eq(\"eye_color\", \"brown\"), gripql.eq(\"eye_color\", \"hazel\"))))) %s != %s" %
            (count, 13)
        )

    count = 0
    for i in G.query().V().has(
            gripql.not_(
                gripql.or_(
                    gripql.eq("_label", "Character"),
                    gripql.eq("name", "Human"),
                )
            )
    ):
        count += 1
        if i['label'] == "Character" or ("name" in i["data"] and i['data']["name"] == "Human"):
            errors.append("Wrong vertex returned %s" % (i))
    if count != 20:
        errors.append(
            "Fail: G.query().V().has(gripql.not_(gripql.and_(gripql.eq(\"_label\", \"Character\"), gripql.eq(\"name\", \"Human\")))) %s != %s" %
            (count, 20)
        )

    count = 0
    for i in G.query().V().has(
            gripql.not_(
                gripql.or_(
                    gripql.eq("_label", "Character"),
                    gripql.or_(
                        gripql.eq("name", "Human"),
                        gripql.contains("terrain", "jungle")
                    )
                )
            )
    ):
        count += 1
        if not i['gid'].startswith("Vehicle:") \
            and not i["gid"].startswith("Starship:") and not i["gid"].startswith("Species:") \
                and not i["gid"].startswith("Planet:") and not i["gid"].startswith("Film:"):
            errors.append("Wrong vertex returned %s" % (i))
    if count != 19:
        errors.append(
            "Fail: G.query().V().has(gripql.not_(gripql.or_(gripql.eq(\"_label\", \"Character\"), gripql.or_(gripql.eq(\"name\", \"Human\"),  gripql.contains(\"terrain\", \"jungle\"))))) %s != %s" %
            (count, 19)
        )

    count = 0
    for i in G.query().V().has(
            gripql.not_(
                gripql.and_(
                    gripql.eq("_label", "Planet"),
                    gripql.or_(
                        gripql.eq("surface_water", 1),
                        gripql.contains("terrain", "jungle")
                    )
                )
            )
    ):
        count += 1
    if count != 37:
        errors.append(
            "Fail: G.query().V().has(gripql.not_(gripql.and_(gripql.eq(\"_label\", \"Planet\"), gripql.or_(gripql.eq(\"surface_water\", 1),  gripql.contains(\"terrain\", \"jungle\"))))) %s != %s" %
            (count, 37)
        )

    return errors
