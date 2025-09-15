

def test_count(man):
    errors = []

    G = man.setGraph("swapi")

    i = list(G.V().count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.V().count()")
    elif i[0]["count"] != 39:
        errors.append("Fail: G.V().count() %s != %s" % (i[0]["count"], 39))

    i = list(G.V("non-existent").count())
    #print(i)
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.V(\"non-existent\").count()")
    elif i[0]["count"] != 0:
        errors.append("Fail: G.V(\"non-existent\").count() %s != %s" % (i[0]["count"], 0))

    i = list(G.V().outE().count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for O.V().outE().count()")
    elif i[0]["count"] != 144:
        errors.append("Fail: G.V().outE().count() %s != %s" % (i[0]["count"], 144))

    i = list(G.V().outE("non-existent").count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for G.E(\"non-existent\").count()")
    elif i[0]["count"] != 0:
        errors.append("Fail: G.V().outE(\"non-existent\").count() %s != %s" % (i[0]["count"], 0))

    return errors


# tests an edge case where mongo aggregations fill fail to return a count when
# the ccollection doesnt exist
def test_count_when_no_data(man):
    errors = []

    G = man.writeTest()

    i = list(G.V().count())
    #print(i)
    if len(i) < 1:
        errors.append("Fail: nothing returned for G.V().count()")
    elif i[0]["count"] != 0:
        errors.append("Fail: G.V().count() %s != %s" % (i[0]["count"], 0))

    i = list(G.V("non-existent").count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for G.V(\"non-existent\").count()")
    elif i[0]["count"] != 0:
        errors.append("Fail: G.V(\"non-existent\").count() %s != %s" % (i[0]["count"], 0))

    i = list(G.V().outE().count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for G.V().outE().count()")
    elif i[0]["count"] != 0:
        errors.append("Fail: G.V().outE().count() %s != %s" % (i[0]["count"], 0))

    i = list(G.V().outE("non-existent").count())
    if len(i) < 1:
        errors.append("Fail: nothing returned for G.E(\"non-existent\").count()")
    elif i[0]["count"] != 0:
        errors.append("Fail: G.V().outE(\"non-existent\").count() %s != %s" % (i[0]["count"], 0))

    return errors
