def test_distinct(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V().distinct():
        count += 1
    if count != 39:
        errors.append("V().distinct() distinct count %s != %s" % (count, 39))

    count = 0
    for i in G.query().V().distinct("_gid"):
        count += 1
    if count != 39:
        errors.append("""V().distinct("_gid") distinct count %s != %s""" % (count, 39))

    count = 0
    for i in G.query().V().distinct("eye_color"):
        count += 1
    if count != 8:
        errors.append("""V().distinct("eye_color") distinct count %s != %s""" % (count, 8))

    count = 0
    for i in G.query().V().distinct("gender"):
        count += 1
    if count != 4:
        errors.append("""V().distinct("gender") distinct count %s != %s""" % (count, 4))

    count = 0
    for i in G.query().V().distinct("non-existent-field"):
        count += 1
    if count != 0:
        errors.append("Distinct %s != %s" % (count, 0))

    count = 0
    for i in G.query().V().hasLabel("Character").as_("person").out().distinct("$person.name"):
        count += 1
    if count != 18:
        errors.append("Distinct  G.query().V().hasLabel(\"Character\").as_(\"person\").out().distinct(\"$person.name\") %s != %s" % (count, 18))

    count = 0
    for i in G.query().V().hasLabel("Character").as_("person").out().distinct("$person.eye_color"):
        count += 1
    if count != 8:
        errors.append("Distinct  G.query().V().hasLabel(\"Character\").as_(\"person\").out().distinct(\"$person.eye_color\") %s != %s" % (count, 8))

    return errors


def test_distinct_multi(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    o = {}
    for i in G.query().V().as_("a").out().distinct(["$a.eye_color", "_gid"]).render(["$a.eye_color", "_gid"]):
        if i[0] in o and o[i[0]] != i[1]:
            errors.append("Non-unique pair returned: %s" % (i))
        count += 1
    if count != 29:
        errors.append("Distinct multi %s != %s" % (count, 29))

    return errors
