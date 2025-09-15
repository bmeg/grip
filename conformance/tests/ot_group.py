from __future__ import absolute_import

import gripql

def test_childGroups(man):
    errors = []
    G = man.setGraph("swapi")

    mapping = {
        'Planet:1' : ['Luke Skywalker', 'C-3PO', 'Darth Vader', 'Owen Lars', 'Beru Whitesun lars', 'R5-D4', 'Biggs Darklighter'],
        'Planet:2' :  ['Leia Organa', 'Raymus Antilles']
    }
    mapping_hair = {
        'Planet:1' : ["blond", None, "none", "brown, grey", "brown", None, "black"],
        'Planet:2' : ["brown", "brown"]
    }

    for i in G.V().hasLabel("Planet").as_("planet").out("residents").as_("character").select("planet").group( {"people" : "$character.name"}  ):
        #print(i)
        if sorted(i["people"]) != sorted(mapping[i["_id"]]):
            errors.append("grouped output not equal: %s != %s" % (sorted(i["people"]) , sorted(mapping[i["_id"]])))

    for i in G.V().hasLabel("Planet").as_("planet").out("residents").as_("character").select("planet").group(
        {"people" : "$character.name", "hair":"$character.hair_color"}  ):
        #print(i)
        if sorted(i["people"]) != sorted(mapping[i["_id"]]):
            errors.append("grouped output not equal: %s != %s" % (sorted(i["people"]) , sorted(mapping[i["_id"]])))

        if sorted(i["hair"], key=lambda x: (x is None, x)) != sorted(mapping_hair[i["_id"]], key=lambda x: (x is None, x)):
            errors.append("grouped output not equal: %s != %s" % (sorted(i["hair"], key=lambda x: (x is None, x)) , sorted(mapping_hair[i["_id"]], key=lambda x: (x is None, x))))
    return errors
