from __future__ import absolute_import

import gripql

def test_childGroups(man):
    errors = []
    G = man.setGraph("swapi")

    mapping = {
        'Planet:1' : ['Luke Skywalker', 'C-3PO', 'Darth Vader', 'Owen Lars', 'Beru Whitesun lars', 'R5-D4', 'Biggs Darklighter'],
        'Planet:2' :  ['Leia Organa', 'Raymus Antilles']
    }

    for i in G.query().V().hasLabel("Planet").as_("planet").out("residents").as_("character").select("planet").group( {"people" : "$character.name"}  ):
        print(i)
        if sorted(i["data"]["people"]) != sorted(mapping[i["gid"]]):
            errors.append("grouped output not equal: %s != %s" % (sorted(i["data"]["people"]) , sorted(mapping[i["gid"]])))
    return errors