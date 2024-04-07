from __future__ import absolute_import

import gripql


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

def test_returnNil(man):
    errors = []

    G = man.setGraph("swapi")

    #print("query 1")
    count_1 = 0
    for i in G.query().V().hasLabel("Character").outNull("starships"):
        #print(i)
        count_1 += 1

    #print("query 1")
    count_1 = 0
    for i in G.query().V().hasLabel("Character").outENull("starships"):
        #print(i)
        count_1 += 1

    return errors    


def test_hasLabelOut(man):
    errors = []

    G = man.setGraph("swapi")

    #print("query 1")
    count_1 = 0
    for i in G.query().V().hasLabel("Character").as_("a").out("starships").as_("b").render(["$a._gid", "$b._gid", "$b._label"]):
        #print("out", i)
        if i[0] in noStarshipCharacters:
            errors.append("%s should not have been found" % (i[0]))
        count_1 += 1

    #print("query 2")
    count_2 = 0
    nullFound = []
    for i in G.query().V().hasLabel("Character").as_("a").outNull("starships").as_("b").render(["$a._gid", "$b._gid", "$b._label"]):
        #print("outnull", i)
        if i[0] in noStarshipCharacters:
            nullFound.append(i[0])
        count_2 += 1

    if len(nullFound) != len(noStarshipCharacters):
        errors.append("Incorrect null count %d != %d" % (len(nullFound), len(noStarshipCharacters)))
        print(nullFound)
        print(noStarshipCharacters)

    #print(count_1, count_2)
    return errors

def test_hasLabelOutE(man):
    errors = []

    G = man.setGraph("swapi")

    #print("query 1")
    count_1 = 0
    for i in G.query().V().hasLabel("Character").as_("a").outE("starships").as_("b").render(["$a._gid", "$b._gid", "$b._label"]):
        #print("out", i)
        if i[0] in noStarshipCharacters:
            errors.append("%s should not have been found" % (i[0]))
        count_1 += 1

    #print("query 2")
    count_2 = 0
    nullFound = []
    for i in G.query().V().hasLabel("Character").as_("a").outENull("starships").as_("b").render(["$a._gid", "$b._gid", "$b._label"]):
        #print("outnull", i)
        if i[0] in noStarshipCharacters:
            nullFound.append(i[0])
        count_2 += 1

    if len(nullFound) != len(noStarshipCharacters):
        errors.append("Incorrect null count %d != %d" % (len(nullFound), len(noStarshipCharacters)))
        print(nullFound)
        print(noStarshipCharacters)

    #print(count_1, count_2)
    return errors



noResidenceCharacters = [
    "Character:3",
    "Character:10",
    "Character:12",
    "Character:13",
    "Character:14",
    "Character:15",
    "Character:16",
    "Character:18",
    "Character:19"
]


def test_hasLabelIn(man):
    errors = []

    G = man.setGraph("swapi")

    for i in G.query().V().hasLabel("Character").as_("a").in_("residents").as_("b").render(["$a._gid", "$b._gid", "$b._label"]):
        #print("in:", i)
        if i[0] in noResidenceCharacters:
            errors.append("%s should not have been found" % (i[0]))

    nullFound = []
    for i in G.query().V().hasLabel("Character").as_("a").inNull("residents").as_("b").render(["$a._gid", "$b._gid", "$b._label"]):
        #print("inNull:", i)
        if i[0] in noResidenceCharacters:
            nullFound.append(i[0])

    if len(nullFound) != len(noResidenceCharacters):
        errors.append("Incorrect null count %d != %d" % (len(nullFound), len(noResidenceCharacters)))

    return errors


def test_hasLabelInE(man):
    errors = []

    G = man.setGraph("swapi")

    for i in G.query().V().hasLabel("Character").as_("a").inE("residents").as_("b").render(["$a._gid", "$b._gid", "$b._label"]):
        #print("in:", i)
        if i[0] in noResidenceCharacters:
            errors.append("%s should not have been found" % (i[0]))

    nullFound = []
    for i in G.query().V().hasLabel("Character").as_("a").inENull("residents").as_("b").render(["$a._gid", "$b._gid", "$b._label"]):
        #print("inNull:", i)
        if i[0] in noResidenceCharacters:
            nullFound.append(i[0])

    if len(nullFound) != len(noResidenceCharacters):
        errors.append("Incorrect null count %d != %d" % (len(nullFound), len(noResidenceCharacters)))

    return errors
