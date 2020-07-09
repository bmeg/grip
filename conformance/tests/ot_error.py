

def test_fields(G, man):
    errors = []

    man.setGraph("swapi")

    try:
        for i in G.query().out():
            pass
        errors.append("Bad traversal query returned without exception")
    except Exception:
        pass

    return errors
