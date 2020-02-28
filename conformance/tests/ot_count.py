

def test_count(O, man):
    errors = []

    man.setGraph("swapi")

    i = list(O.query().V().count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.query().V().count()")
    elif i[0].count != 25:
        errors.append("Fail: O.query().V().count() %s != %s" % (i[0].count, 25))

    i = list(O.query().V("non-existent").count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.query().V(\"non-existent\").count()")
    elif i[0].count != 0:
        errors.append("Fail: O.query().V(\"non-existent\").count() %s != %s" % (i[0].count, 0))

    i = list(O.query().E().count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.query().E().count()")
    elif i[0].count != 14:
        errors.append("Fail: O.query().E().count() %s != %s" % (i[0].count, 14))

    i = list(O.query().E("non-existent").count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.query().E(\"non-existent\").count()")
    elif i[0].count != 0:
        errors.append("Fail: O.query().E(\"non-existent\").count() %s != %s" % (i[0].count, 0))

    return errors


# tests an edge case where mongo aggregations fill fail to return a count when
# the ccollection doesnt exist
def test_count_when_no_data(O, man):
    errors = []

    i = list(O.query().V().count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.query().V().count()")
    elif i[0].count != 0:
        errors.append("Fail: O.query().V().count() %s != %s" % (i[0].count, 0))

    i = list(O.query().V("non-existent").count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.query().V(\"non-existent\").count()")
    elif i[0].count != 0:
        errors.append("Fail: O.query().V(\"non-existent\").count() %s != %s" % (i[0].count, 0))

    i = list(O.query().E().count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.query().E().count()")
    elif i[0].count != 0:
        errors.append("Fail: O.query().E().count() %s != %s" % (i[0].count, 0))

    i = list(O.query().E("non-existent").count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.query().E(\"non-existent\").count()")
    elif i[0].count != 0:
        errors.append("Fail: O.query().E(\"non-existent\").count() %s != %s" % (i[0].count, 0))

    return errors
