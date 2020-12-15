
def test_fields(man):
    errors = []

    G = man.setGraph("swapi")

    expected = {
        u"gid": u"Character:1",
        u"label": u"Character",
        u"data": {u"name": u"Luke Skywalker"}
    }
    resp = G.query().V("Character:1").fields(["name"]).execute()
    if resp[0] != expected:
        errors.append("vertex contains incorrect fields: \nexpected:%s\nresponse:%s" % (expected, resp))

    expected = {
        u"gid": u"Character:1",
        u"label": u"Character",
        u"data": {}
    }
    resp = G.query().V("Character:1").fields(["non-existent"]).execute()
    if resp[0] != expected:
        errors.append("vertex contains incorrect fields: \nexpected:%s\nresponse:%s" % (expected, resp))

    return errors
