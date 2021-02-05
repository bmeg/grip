from __future__ import absolute_import

import requests
import json


def vertex_compare(val, expected):
    if val["gid"] != expected["gid"]:
        return False
    if val["label"] != expected["label"]:
        return False
    for k in expected['data']:
        if expected['data'][k] != val['data'].get(k, None):
            return False
    return True


def edge_compare(val, expected):
    if val["gid"] != expected["gid"]:
        return False
    if val["to"] != expected["to"]:
        return False
    if val["from"] != expected["from"]:
        return False
    if val["label"] != expected["label"]:
        return False
    for k in expected['data']:
        if expected['data'][k] != val['data'].get(k, None):
            return False
    return True


def test_get_vertex(man):
    errors = []

    G = man.setGraph("swapi")

    expected = {
        "gid": "Character:1",
        "label": "Character",
        "data": {
            "system": {
                "created": "2014-12-09T13:50:51.644000Z",
                "edited": "2014-12-20T21:17:56.891000Z"
            },
            "name": "Luke Skywalker",
            "height": 172,
            "mass": 77,
            "hair_color": "blond",
            "skin_color": "fair",
            "eye_color": "blue",
            "birth_year": "19BBY",
            "gender": "male",
            "url": "https://swapi.co/api/people/1/"
        }
    }

    try:
        resp = G.getVertex("Character:1")
        if not vertex_compare(resp, expected):
            errors.append("Wrong vertex \n %s !=\n %s" % (json.dumps(resp, indent=4), json.dumps(expected, indent=4)))
    except Exception as e:
        errors.append("Unexpected error %s: %s" % (type(e).__name__, e))

    try:
        G.getVertex("i-dont-exist")
        errors.append("Expected HTTPError")
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            errors.append(
                "Expected 404 not %s: %s" % (e.response.status_code, e)
            )
    return errors


def test_get_edge(man):
    errors = []

    G = man.setGraph("swapi")

    expected = {
        "gid": "Film:1-characters-Character:1",
        "label": "characters",
        "from": "Film:1",
        "to": "Character:1",
        "data": {}
    }

    try:
        resp = G.getEdge("Film:1-characters-Character:1")
        if not edge_compare(resp, expected):
            errors.append("Wrong edge %s != %s" % (resp, expected))
    except Exception as e:
        errors.append("Unexpected error %s: %s" % (type(e).__name__, e))

    try:
        G.getEdge("i-dont-exist")
        errors.append("Expected 404")
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            errors.append(
                "Expected 404 not %s: %s" % (e.response.status_code, e)
            )

    return errors


def test_V(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V():
        count += 1
    if count != 39:
        errors.append("Fail: G.query().V() %s != %s" % (count, 25))

    count = 0
    for i in G.query().V("Character:1"):
        count += 1
        if i['gid'] != "Character:1":
            errors.append(
                "Fail: G.query().V(\"Character:1\") - Wrong vertex %s" % (i['gid'])
            )
    if count != 1:
        errors.append("Fail: G.query().V(\"Character:1\") %s != %s" % (count, 1))

    return errors


def test_E(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().E():
        count += 1
    if count != 144:
        errors.append("Fail: G.query().E() %s != %d" % (count, 144))

    count = 0
    for i in G.query().E("Film:1-characters-Character:1"):
        if i['gid'] != "Film:1-characters-Character:1":
            errors.append(
                "Fail: G.query().E(\"Film:1-characters-Character:1\") - Wrong edge %s" % (i['gid'])
            )
        count += 1
    if count != 1:
        errors.append(
            "Fail: G.query().E(\"Film:1-characters-Character:1\") %s != %d" % (count, 1))

    return errors


def test_outgoing(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V("Starship:12").out():
        if i['gid'] not in ['Character:1', 'Character:18', 'Character:19', 'Character:9', 'Film:1']:
            errors.append(
                "Fail: G.query().V(\"Starship:12\").out() - Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 5:
        errors.append(
            "Fail: G.query().V(\"Starship:12\").out() %s != %d" % (count, 5))

    count = 0
    for i in G.query().V("Starship:12").out("pilots"):
        if i['gid'] not in ['Character:1', 'Character:18', 'Character:19', 'Character:9']:
            errors.append(
                "Fail: G.query().V(\"Starship:12\").out(\"pilots\") - Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 4:
        errors.append(
            "Fail: G.query().V(\"Starship:12\").out(\"pilots\") %s != %d" % (count, 4)
        )

    count = 0
    for i in G.query().E("Film:1-characters-Character:1").out():
        if i['gid'] != "Character:1":
            errors.append(
                "Fail: G.query().E(\"Film:1-characters-Character:1\").out() - Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 1:
        errors.append(
            "Fail: G.query().E(\"Film:1-characters-Character:1\").out() %s != %d" % (count, 1)
        )

    return errors


def test_incoming(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V("Starship:12").in_():
        if i['gid'] not in ['Character:1', 'Character:18', 'Character:19', 'Character:9', 'Film:1']:
            errors.append(
                "Fail: G.query().V(\"Starship:12\").in_() - Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 5:
        errors.append(
            "Fail: G.query().V(\"Starship:12\").in_() %s != %d" % (count, 5))

    count = 0
    for i in G.query().V("Starship:12").in_("starships"):
        if i['gid'] not in ['Character:1', 'Character:18', 'Character:19', 'Character:9', 'Film:1']:
            errors.append(
                "Fail: G.query().V(\"Starship:12\").in_(\"starships\") - Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 5:
        errors.append(
            "Fail: G.query().V(\"Starship:12\").in_(\"starships\") %s != %d" % (count, 5)
        )

    # sanity check since vertices are connected by multipled edges
    count = 0
    for i in G.query().V("Starship:12").in_("pilots"):
        count += 0
    if count != 0:
        errors.append(
            "Fail: G.query().V(\"Starship:12\").in_(\"piolots\") %s != %d" % (count, 0)
        )

    count = 0
    for i in G.query().E("Film:1-characters-Character:1").in_():
        if i['gid'] != "Film:1":
            errors.append(
                "Fail: G.query().E(\"Film:1-characters-Character:1\").in_() - Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 1:
        errors.append(
            "Fail: G.query().E(\"Film:1-characters-Character:1\").in_() %s != %d" % (count, 1)
        )

    return errors


def test_outgoing_edge(man):
    errors = []

    G = man.setGraph("swapi")

    c = G.query().V("Character:1").outE().count().execute()[0]["count"]
    if c != 4:
        errors.append("Fail: G.query().V(\"Character:1\").outE().count() %d != %d" % (c, 4))

    for i in G.query().V("Character:1").outE():
        if not i['gid'].startswith("Character:1"):
            errors.append("Fail: G.query().V(\"Character:1\").outE() - \
            Wrong edge '%s'" % (i['gid']))

    for i in G.query().V("Character:1").outE().out():
        if i['gid'] not in ['Film:1', 'Planet:1', 'Species:1', 'Starship:12']:
            errors.append("Fail: G.query().V(\"Character:1\").outE().out() - \
            Wrong vertex %s" % (i['gid']))

    c = G.query().V("Character:1").outE("homeworld").count().execute()[0]["count"]
    if c != 1:
        errors.append("Fail: G.query().V(\"Character:1\").outE(\"homeworld\").count() - %s != %s" % (c, 1))

    return errors


def test_incoming_edge(man):
    errors = []

    G = man.setGraph("swapi")

    c = G.query().V("Character:1").inE().count().execute()[0]["count"]
    if c != 4:
        errors.append("Fail: G.query().V(\"Character:1\").inE().count() %d != %d" % (c, 4))

    for i in G.query().V("Character:1").inE():
        if not i['gid'].endswith("Character:1"):
            errors.append("Fail: G.query().V(\"Character:1\").inE() - \
            Wrong edge %s" % (i['gid']))

    for i in G.query().V("Character:1").inE().in_():
        if i['gid'] not in ['Film:1', 'Planet:1', 'Species:1', 'Starship:12']:
            errors.append("Fail: G.query().V(\"Character:1\").inE().in() - \
            Wrong vertex %s" % (i['gid']))

    c = G.query().V("Character:1").inE("residents").count().execute()[0]["count"]
    if c != 1:
        errors.append("Fail: G.query().V(\"Character:1\").inE(\"residents\").count() - %s != %s" % (c, 1))

    return errors


def test_outgoing_edge_all(man):
    errors = []
    G = man.setGraph("swapi")
    for i in G.query().V().as_("a").outE().as_("b").render(["$a._gid", "$b._from", "$b._to", "$b._gid"]):
        if i[0] != i[1]:
            errors.append("outE _gid/from missmatch %s != %s" % (i[0], i[1]))
        if i[1] == i[2]:
            errors.append("outE to/from the same %s == %s" % (i[1], i[2]))
        if not i[3].startswith(i[0]):
            errors.append("outE _gid prefix %s != %s" % (i[3], i[0]))
    return errors


def test_incoming_edge_all(man):
    errors = []
    G = man.setGraph("swapi")
    for i in G.query().V().as_("a").inE().as_("b").render(["$a._gid", "$b._to", "$b._gid"]):
        if i[0] != i[1]:
            errors.append("inE _gid/to missmatch %s != %s" % (i[0], i[1]))
        if not i[2].endswith(i[0]):
            errors.append("inE _gid wrong suffix %s != %s" % (i[2], i[0]))
    return errors


def test_out_edge_out_all(man):
    errors = []
    G = man.setGraph("swapi")
    for i in G.query().V().as_("a").outE().as_("b").out().as_("c").render(["$a._gid", "$b._from", "$b._to", "$c._gid"]):
        if i[0] != i[1]:
            errors.append("outE-out _gid/from missmatch %s != %s" % (i[0], i[1]))
        if i[2] != i[3]:
            errors.append("outE-out to/_gid missmatch %s != %s" % (i[0], i[1]))
    return errors


def test_in_out_equal(man):
    G = man.setGraph("swapi")
    errors = []
    count1 = 0
    for i in G.query().V().out():
        count1 += 1

    count2 = 0
    for i in G.query().V().in_():
        count2 += 1

    if count1 != count2:
        errors.append("in / out counts not the same %s != %s" % (count1, count2))
    return errors


def test_ine_oute_equal(man):
    G = man.setGraph("swapi")
    errors = []
    count1 = 0
    for i in G.query().V().outE():
        count1 += 1

    count2 = 0
    for i in G.query().V().inE():
        count2 += 1

    if count1 != count2:
        errors.append("inE / outE counts not the same %s != %s" % (count1, count2))
    return errors


def test_both(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for i in G.query().V("Starship:12").both():
        if i['gid'] not in ['Character:1', 'Character:18', 'Character:19', 'Character:9', 'Film:1']:
            errors.append(
                "Fail: G.query().V(\"Starship:12\").both() - \
                Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 10:
        errors.append(
            "Fail: G.query().V(\"Starship:12\").both() %s != %d" % (count, 10))

    count = 0
    for i in G.query().V("Starship:12").both(["pilots", "starships"]):
        if i['gid'] not in ['Character:1', 'Character:18', 'Character:19', 'Character:9', 'Film:1']:
            errors.append(
                "Fail: G.query().V(\"Starship:12\").both([\"pilots\", \"starships\"]) - \
                Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 9:
        errors.append(
            "Fail: G.query().V(\"Starship:12\").both([\"pilots\", \"starships\"]) %s != %d" % (count, 9)
        )

    count = 0
    for i in G.query().E("Film:1-characters-Character:1").both():
        if i['gid'] not in ["Film:1", "Character:1"]:
            errors.append(
                "Fail: G.query().E(\"Film:1-characters-Character:1\").both() - \
                Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 2:
        errors.append(
            "Fail: G.query().E(\"Film:1-characters-Character:1\").both() %s != %d" % (count, 2)
        )

    return errors


def test_both_edge(man):
    errors = []

    G = man.setGraph("swapi")

    c = G.query().V("Character:1").bothE().count().execute()[0]["count"]
    if c != 8:
        errors.append("Fail: G.query().V(\"Character:1\").bothE().count() %d != %d" % (c, 8))

    for i in G.query().V("Character:1").inE():
        if not (i['gid'].startswith("Character:1") or i['gid'].endswith("Character:1")):
            errors.append("Fail: G.query().V(\"Character:1\").bothE() - \
            Wrong edge %s" % (i['gid']))

    for i in G.query().V("Character:1").bothE().out():
        if i['gid'] not in ['Character:1', 'Character:1', 'Character:1', 'Character:1', 'Film:1', 'Planet:1', 'Species:1', 'Starship:12']:
            errors.append("Fail: G.query().V(\"Character:1\").bothE().out() - \
            Wrong vertex %s" % (i['gid']))

    c = G.query().V("Character:1").bothE(["homeworld", "residents"]).count().execute()[0]["count"]
    if c != 2:
        errors.append("Fail: G.query().V(\"Character:1\").inE([\"homeworld\", \"residents\"]).count() - %s != %s" % (c, 2))

    return errors


def test_limit(man):
    errors = []

    G = man.setGraph("swapi")

    tests = [
        "G.query().V().limit(3)",
        "G.query().E().limit(3)"
    ]

    expected_results = [
        list(i["gid"] for i in G.query().V().execute())[:3],
        list(i["gid"] for i in G.query().E().execute())[:3]
    ]

    for test, expected in zip(tests, expected_results):
        results = eval(test).execute()
        actual = [x["gid"] for x in results]

        # check contents
        for x in actual:
            if x not in expected:
                errors.append("Fail: %s - unexpected result - %s" % (test, x))

        # check number of results
        if len(actual) != len(expected):
            errors.append("Fail: %s - unexpected result count - \
            %s != %s" % (test, len(actual), len(expected)))

        # check order
        if actual != expected:
            errors.append("Fail: %s - unexpected order - \
            %s != %s" % (test, actual, expected))

    return errors


def test_skip(man):
    errors = []

    G = man.setGraph("swapi")

    tests = [
        "G.query().V().skip(3).limit(3)",
        "G.query().E().skip(3).limit(3)"
    ]

    expected_results = [
        list(i["gid"] for i in G.query().V().execute())[3:6],
        list(i["gid"] for i in G.query().E().execute())[3:6]
    ]

    for test, expected in zip(tests, expected_results):
        results = eval(test).execute()
        actual = [x["gid"] for x in results]

        # check contents
        for x in actual:
            if x not in expected:
                errors.append("Fail: %s - unexpected result - %s" % (test, x))

        # check number of results
        if len(actual) != len(expected):
            errors.append("Fail: %s - unexpected result count - \
            %s != %s" % (test, len(actual), len(expected)))

        # check order
        if actual != expected:
            errors.append("Fail: %s - unexpected order - \
            %s != %s" % (test, actual, expected))

    return errors


def test_range(man):
    errors = []

    G = man.setGraph("swapi")

    tests = [
        "G.query().V().range(3, 5)",
        "G.query().V().range(34, -1)",
        "G.query().E().range(120, 123)",
        "G.query().E().range(140, -1)"
    ]

    expected_results = [
        list(i["gid"] for i in G.query().V().execute())[3:5],
        list(i["gid"] for i in G.query().V().execute())[34:],
        list(i["gid"] for i in G.query().E().execute())[120:123],
        list(i["gid"] for i in G.query().E().execute())[140:]
    ]

    for test, expected in zip(tests, expected_results):
        results = eval(test).execute()
        actual = [x["gid"] for x in results]

        # check contents
        for x in actual:
            if x not in expected:
                errors.append("Fail: %s - unexpected result - %s" % (test, x))

        # check number of results
        if len(actual) != len(expected):
            errors.append("Fail: %s - unexpected result count - \
            %s != %s" % (test, len(actual), len(expected)))

        # check order
        if actual != expected:
            errors.append("Fail: %s - unexpected order - \
            %s != %s" % (test, actual, expected))

    return errors
