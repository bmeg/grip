from __future__ import absolute_import

import gripql

def test_sort_name(man):
    errors = []

    G = man.setGraph("swapi")

    q = G.query().V().hasLabel("Character").sort( "name" )
    last = ""
    for row in q:
        #print(row)
        if row["data"]["name"] < last:
            errors.append("incorrect sort: %s < %s" % (row["data"]["name"], last))
        last = row["data"]["name"]
    return errors



def test_sort_units(man):
    errors = []
    G = man.setGraph("swapi")

    q = G.query().V().hasLabel("Vehicle").sort( "max_atmosphering_speed" )
    last = 0
    for row in q:
        #print(row)
        value = row["data"]["max_atmosphering_speed"]
        if value < last:
            errors.append("incorrect sort: %s < %s" % (value, last))
        last = value

   
    q = G.query().V().hasLabel("Vehicle").sort( "max_atmosphering_speed", decending=True )
    last = 1000000000
    for row in q:
        print(row)
        value = row["data"]["max_atmosphering_speed"]
        if value > last:
            errors.append("incorrect sort: %s > %s" % (value, last))
        last = value

    return errors

