

def test_fields(man):
    errors = []

    G = man.setGraph("swapi")

    try:
        for i in G.out():
            pass
        errors.append("Bad traversal query returned without exception")
    except Exception:
        pass

    return errors
