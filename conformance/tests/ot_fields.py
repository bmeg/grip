
def test_fields(man):
    errors = []

    G = man.setGraph("swapi")

    expected = {
        u"_id": u"Character:1",
        u"_label": u"Character",
        u"name": u"Luke Skywalker"
    }
    resp = G.query().V("Character:1").fields(["name"]).execute()
    if resp[0] != expected:
        errors.append("""Query 'V("Character:1").fields(["name"])' vertex contains incorrect fields: \nexpected:%s\nresponse:%s""" % (expected, resp))

    expected = {
        u"_id": u"Character:1",
        u"_label": u"Character",
    }
    resp = G.query().V("Character:1").fields(["non-existent"]).execute()
    if resp[0] != expected:
        errors.append("vertex contains incorrect fields: \nexpected:%s\nresponse:%s" % (expected, resp))

    return errors
