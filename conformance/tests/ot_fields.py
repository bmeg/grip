
def test_fields(O, man):
    errors = []

    man.setGraph("swapi")

    expected = {
        u"gid": u"10",
        u"label": u"Character",
        u"data": {u"name": u"han"}
    }
    resp = O.query().V("10").fields(["name"]).execute()
    if resp[0] != expected:
        errors.append("vertex contains incorrect fields: \nexpected:%s\nresponse:%s" % (expected, resp))

    expected = {
        u"gid": u"10",
        u"label": u"Character",
        u"data": {}
    }
    resp = O.query().V("10").fields(["non-existent"]).execute()
    if resp[0] != expected:
        errors.append("vertex contains incorrect fields: \nexpected:%s\nresponse:%s" % (expected, resp))

    return errors
