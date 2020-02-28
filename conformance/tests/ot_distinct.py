def test_distinct(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V().distinct():
        count += 1
    if count != 25:
        errors.append("Distinct %s != %s" % (count, 25))

    count = 0
    for i in O.query().V().distinct("_gid"):
        count += 1
    if count != 25:
        errors.append("Distinct %s != %s" % (count, 25))

    count = 0
    for i in O.query().V().distinct("name"):
        count += 1
    if count != 23:
        errors.append("Distinct %s != %s" % (count, 23))

    count = 0
    for i in O.query().V().distinct("lang"):
        count += 1
    if count != 3:
        errors.append("Distinct %s != %s" % (count, 3))

    count = 0
    for i in O.query().V().distinct("non-existent-field"):
        count += 1
    if count != 0:
        errors.append("Distinct %s != %s" % (count, 0))

    count = 0
    for i in O.query().V().hasLabel("Person").as_("person").out().distinct("$person.name"):
        count += 1
    if count != 5:
        errors.append("Distinct  O.query().V().hasLabel(\"Person\").as_(\"person\").out().distinct(\"$person.name\") %s != %s" % (count, 5))

    return errors


def test_distinct_multi(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V().as_("a").out().distinct(["$a.name", "_gid"]).render(["$a.name", "_gid"]):
        count += 1
    if count != 14:
        errors.append("Distinct multi %s != %s" % (count, 14))

    return errors
