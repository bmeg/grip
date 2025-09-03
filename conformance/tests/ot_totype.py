import gripql

def test_totype(man):
    errors = []

    G = man.setGraph("swapi")

    q = G.V().hasLabel("Character").totype("birth_year", "string").totype("eye_color", "float").totype("hair_color", "int").totype("skin_color", "list").totype("mass", "string").execute()
    if len(q) == 0:
        errors.append("ERROR, q returns no items")
    for row in q:
        data = row
        # transforms that should work
        if not isinstance(data["birth_year"], str):
            errors.append("string field %s should be string" % (data["birth_year"]))
        if not isinstance(data["skin_color"], list):
            errors.append("string field %s should be list" % (data["skin_color"]))
        # some masses are none by default
        if data["mass"] is not None and not isinstance(data["mass"], str):
            errors.append("int field %d should be string" % (data["mass"]))

        # transforms that shouldn't work'
        if data["eye_color"] != 0.0:
            errors.append("string value %s should be 0.0" % (data["eye_color"]))
        if data["hair_color"] != 0:
            errors.append("%s should be 0" % (data["hair_color"]))

    r = G.V().hasLabel("Starship").totype("hyperdrive_rating", "string").totype("length", "int").totype("length", "float").totype("system", "list").execute()
    if len(r) == 0:
        errors.append("ERROR, r returns no items")
    for row in r:
        data = row
        if  isinstance(data["hyperdrive_rating"], int) or isinstance(data["hyperdrive_rating"], float):
            errors.append("float field %s should be int or float" %(data["hyperdrive_rating"]))
        if not isinstance(data["length"], int):
            errors.append("float field %s should be int" %(data["length"]))
        if not isinstance(data["system"], list):
            errors.append("dict object with key 'system' %s should be list" %(data["system"]))

    s = G.V().hasLabel("Species").totype("system.created", "bool").execute()
    if len(s) == 0:
        errors.append("ERROR, s returns no items")
    for row in s:
        data = row
        if data["system"]["created"] != True:
            errors.append("string %s to bool should be False" %(data["system"]["created"]))


    t = G.V().hasLabel("Starship").totype("MGLT", "bool").totype("eye_colors", "int").totype("classification", "int").execute()
    if len(t) == 0:
        errors.append("ERROR, t returns no items")

    for row in t:
        data = row
        if data["length"] is False:
            errors.append("int %s to bool should be True" %(data["MGLT"]))
        if data["eye_colors"] != 0:
            errors.append("list %s to bool should be False" %(data["eye_colors"]))
        if data["classification"] !=  0:
            errors.append("string %s to bool should be False" %(data["classification"]))

    return errors
