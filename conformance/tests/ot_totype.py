import gripql

def test_type(man):
    errors = []

    G = man.setGraph("swapi")

    q = G.query().V().hasLabel("Character").totype("birth_year", "string").totype("eye_color", "float").totype("gender", "bool").totype("hair_color", "int").totype("skin_color", "list").totype("mass", "string")
    for row in q:
        data = row["data"]
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
        if data["gender"] != False:
            errors.append("%s should be false" % (data["gender"]))
        if data["hair_color"] != 0:
            errors.append("%d should be 0" % (data["hair_color"]))

    r = G.query().V().hasLabel("Starship").totype("hyperdrive_rating", "string").totype("hyperdrive_rating", "float").totype("length", "int").totype("length", "float").totype("system", "list")
    for row in r:
        data = row["data"]
        if data["hyperdrive_rating"] == 0:
            errors.append("float field converted to string and back to float should be non 0 field")
        if not isinstance(data["length"], int):
            errors.append("float field converted to int and back to float should be int")
        if not isinstance(data["system"], list):
            errors.append("dict object 'system' should be list")

    s = G.query().V().hasLabel("Species").totype("system.created", "bool")
    for row in s:
        data = row["data"]
        if data["system"]["created"] != False:
            errors.append("string %s to bool should be False" %(data["system"]["created"]))


    t = G.query().V().hasLabel("Starship").totype("MGLT", "bool").totype("eye_colors", "int").totype("classification", "int")

    for row in t:
        data = row["data"]
        if data["length"] is False:
            errors.append("int %d to bool should be True" %(data["MGLT"]))
        if data["eye_colors"] != 0:
            errors.append("list %d to bool should be False" %(data["eye_colors"]))
        if data["classification"] !=  0:
            errors.append("string %d to bool should be False" %(data["classification"]))

    return errors
