from __future__ import absolute_import

import gripql


def test_hasLabel(man):
    errors = []

    G = man.setGraph("swapi")

    print("query 1")
    count_1 = 0
    for i in G.query().V().hasLabel("Character"):
        print(i["gid"])
        count_1 += 1

    print("query 2")
    count_2 = 0
    for i in G.query().V().hasLabel("Character").as_("a").outNull("starships").select("a"):
        print(i["gid"])
        count_2 += 1

    print(count_1, count_2)

    return errors