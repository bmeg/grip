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

