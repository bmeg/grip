

def test_count(man):
    errors = []

    G = man.setGraph("swapi")

    i = list(G.query().V().count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for G.query().V().count()")
    elif i[0].count != 39:
        errors.append("Fail: G.query().V().count() %s != %s" % (i[0].count, 39))

    i = list(G.query().V("non-existent").count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for G.query().V(\"non-existent\").count()")
    elif i[0].count != 0:
        errors.append("Fail: G.query().V(\"non-existent\").count() %s != %s" % (i[0].count, 0))

    i = list(G.query().E().count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for G.query().E().count()")
    elif i[0].count != 144:
        errors.append("Fail: G.query().E().count() %s != %s" % (i[0].count, 144))

    i = list(G.query().E("non-existent").count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for G.query().E(\"non-existent\").count()")
    elif i[0].count != 0:
        errors.append("Fail: G.query().E(\"non-existent\").count() %s != %s" % (i[0].count, 0))

    return errors


# tests an edge case where mongo aggregations fill fail to return a count when
# the ccollection doesnt exist
def test_count_when_no_data(man):
    errors = []

    G = man.writeTest()

    i = list(G.query().V().count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for G.query().V().count()")
    elif i[0].count != 0:
        errors.append("Fail: G.query().V().count() %s != %s" % (i[0].count, 0))

    i = list(G.query().V("non-existent").count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for G.query().V(\"non-existent\").count()")
    elif i[0].count != 0:
        errors.append("Fail: G.query().V(\"non-existent\").count() %s != %s" % (i[0].count, 0))

    i = list(G.query().E().count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for G.query().E().count()")
    elif i[0].count != 0:
        errors.append("Fail: G.query().E().count() %s != %s" % (i[0].count, 0))

    i = list(G.query().E("non-existent").count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for G.query().E(\"non-existent\").count()")
    elif i[0].count != 0:
        errors.append("Fail: G.query().E(\"non-existent\").count() %s != %s" % (i[0].count, 0))

    return errors
