from __future__ import absolute_import

import gripql


def test_hasLabelOut(man):
    errors = []

    G = man.setGraph("swapi")

    noStarshipCharacters = [
        "Character:2",
        "Character:3",
        "Character:5",
        "Character:6",
        "Character:7",
        "Character:8",
        "Character:10",
        "Character:12",
        "Character:15",
        "Character:16"
    ]

    #print("query 1")
    count_1 = 0
    for i in G.query().V().hasLabel("Character").as_("a").out("starships").as_("b").render(["$a._gid", "$b._gid", "$b._label"]):
        #print(i)
        if i[0] in noStarshipCharacters:
            errors.append("%s should not have been found" % (i[0]))
        count_1 += 1

    print("query 2")
    count_2 = 0
    nullFound = []
    for i in G.query().V().hasLabel("Character").as_("a").outNull("starships").as_("b").render(["$a._gid", "$b._gid", "$b._label"]):
        #print(i)
        if i[0] in noStarshipCharacters:
            nullFound.append(i[0])
        count_2 += 1

    if len(nullFound) != len(noStarshipCharacters):
        errors.append("Incorrect null count %d != %d" % (len(nullFound), len(noStarshipCharacters)))
        print(nullFound)
        print(noStarshipCharacters)

    #print(count_1, count_2)

    return errors