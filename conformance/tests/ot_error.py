

def test_fields(O, man):
    errors = []

    man.setGraph("swapi")

    try:
        for i in O.query().out():
            pass
        errors.append("Bad traversal query returned without exception")
    except Exception:
        pass

    return errors
