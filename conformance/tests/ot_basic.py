from __future__ import absolute_import

import requests


def test_get_vertex(O, man):
    errors = []

    man.setGraph("swapi")

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
        resp = O.getVertex("Character:1")
        if resp != expected:
            errors.append("Wrong vertex %s != %s" % (resp, expected))
    except Exception as e:
        errors.append("Unexpected error %s: %s" % (type(e).__name__, e))

    try:
        O.getVertex("i-dont-exist")
        errors.append("Expected HTTPError")
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            errors.append(
                "Expected 404 not %s: %s" % (e.response.status_code, e)
            )
    return errors


def test_get_edge(O, man):
    errors = []

    man.setGraph("swapi")

    expected = {
        "gid": "(Film:1)-[characters]->(Character:1)",
        "label": "characters",
        "from": "Film:1",
        "to": "Character:1",
        "data": {}
    }

    try:
        resp = O.getEdge("(Film:1)-[characters]->(Character:1)")
        if resp != expected:
            errors.append("Wrong edge %s != %s" % (resp, expected))
    except Exception as e:
        errors.append("Unexpected error %s: %s" % (type(e).__name__, e))

    try:
        O.getEdge("i-dont-exist")
        errors.append("Expected 404")
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            errors.append(
                "Expected 404 not %s: %s" % (e.response.status_code, e)
            )

    return errors


def test_V(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V():
        count += 1
    if count != 39:
        errors.append("Fail: O.query().V() %s != %s" % (count, 25))

    count = 0
    for i in O.query().V("Character:1"):
        count += 1
        if i['gid'] != "Character:1":
            errors.append(
                "Fail: O.query().V(\"Character:1\") - Wrong vertex %s" % (i['gid'])
            )
    if count != 1:
        errors.append("Fail: O.query().V(\"Character:1\") %s != %s" % (count, 1))

    return errors


def test_E(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().E():
        count += 1
    if count != 144:
        errors.append("Fail: O.query().E() %s != %d" % (count, 144))

    count = 0
    for i in O.query().E("(Film:1)-[characters]->(Character:1)"):
        if i['gid'] != "(Film:1)-[characters]->(Character:1)":
            errors.append(
                "Fail: O.query().E(\"(Film:1)-[characters]->(Character:1)\") - Wrong edge %s" % (i['gid'])
            )
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().E(\"(Film:1)-[characters]->(Character:1)\") %s != %d" % (count, 1))

    return errors


def test_outgoing(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V("Starship:12").out():
        if i['gid'] not in ['Character:1', 'Character:18', 'Character:19', 'Character:9', 'Film:1']:
            errors.append(
                "Fail: O.query().V(\"Starship:12\").out() - Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 5:
        errors.append(
            "Fail: O.query().V(\"Starship:12\").out() %s != %d" % (count, 5))

    count = 0
    for i in O.query().V("Starship:12").out("pilots"):
        if i['gid'] not in ['Character:1', 'Character:18', 'Character:19', 'Character:9']:
            errors.append(
                "Fail: O.query().V(\"Starship:12\").out(\"pilots\") - Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 4:
        errors.append(
            "Fail: O.query().V(\"Starship:12\").out(\"pilots\") %s != %d" % (count, 4)
        )

    count = 0
    for i in O.query().E("(Film:1)-[characters]->(Character:1)").out():
        if i['gid'] != "Character:1":
            errors.append(
                "Fail: O.query().E(\"(Film:1)-[characters]->(Character:1)\").out() - Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().E(\"(Film:1)-[characters]->(Character:1)\").out() %s != %d" % (count, 1)
        )

    return errors


def test_incoming(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V("Starship:12").in_():
        if i['gid'] not in ['Character:1', 'Character:18', 'Character:19', 'Character:9', 'Film:1']:
            errors.append(
                "Fail: O.query().V(\"Starship:12\").in_() - Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 5:
        errors.append(
            "Fail: O.query().V(\"Starship:12\").in_() %s != %d" % (count, 5))

    count = 0
    for i in O.query().V("Starship:12").in_("starships"):
        if i['gid'] not in ['Character:1', 'Character:18', 'Character:19', 'Character:9', 'Film:1']:
            errors.append(
                "Fail: O.query().V(\"Starship:12\").in_(\"starships\") - Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 5:
        errors.append(
            "Fail: O.query().V(\"Starship:12\").in_(\"starships\") %s != %d" % (count, 5)
        )

    # sanity check since vertices are connected by multipled edges
    count = 0
    for i in O.query().V("Starship:12").in_("pilots"):
        count += 0
    if count != 0:
        errors.append(
            "Fail: O.query().V(\"Starship:12\").in_(\"piolots\") %s != %d" % (count, 0)
        )

    count = 0
    for i in O.query().E("(Film:1)-[characters]->(Character:1)").in_():
        if i['gid'] != "Film:1":
            errors.append(
                "Fail: O.query().E(\"(Film:1)-[characters]->(Character:1)\").in_() - Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 1:
        errors.append(
            "Fail: O.query().E(\"(Film:1)-[characters]->(Character:1)\").in_() %s != %d" % (count, 1)
        )

    return errors


def test_outgoing_edge(O, man):
    errors = []

    man.setGraph("swapi")

    c = O.query().V("Character:1").outE().count().execute()[0]["count"]
    if c != 4:
        errors.append("Fail: O.query().V(\"Character:1\").outE().count() %d != %d" % (c, 4))

    for i in O.query().V("Character:1").outE():
        if not i['gid'].startswith("(Character:1)"):
            errors.append("Fail: O.query().V(\"Character:1\").outE() - \
            Wrong edge %s" % (i['gid']))

    for i in O.query().V("Character:1").outE().out():
        if i['gid'] not in ['Film:1', 'Planet:1', 'Species:1', 'Starship:12']:
            errors.append("Fail: O.query().V(\"Character:1\").outE().out() - \
            Wrong vertex %s" % (i['gid']))

    c = O.query().V("Character:1").outE("homeworld").count().execute()[0]["count"]
    if c != 1:
        errors.append("Fail: O.query().V(\"Character:1\").outE(\"homeworld\").count() - %s != %s" % (c, 1))

    return errors


def test_incoming_edge(O, man):
    errors = []

    man.setGraph("swapi")

    c = O.query().V("Character:1").inE().count().execute()[0]["count"]
    if c != 4:
        errors.append("Fail: O.query().V(\"Character:1\").inE().count() %d != %d" % (c, 4))

    for i in O.query().V("Character:1").inE():
        if not i['gid'].endswith("(Character:1)"):
            errors.append("Fail: O.query().V(\"Character:1\").inE() - \
            Wrong edge %s" % (i['gid']))

    for i in O.query().V("Character:1").inE().in_():
        if i['gid'] not in ['Film:1', 'Planet:1', 'Species:1', 'Starship:12']:
            errors.append("Fail: O.query().V(\"Character:1\").inE().in() - \
            Wrong vertex %s" % (i['gid']))

    c = O.query().V("Character:1").inE("residents").count().execute()[0]["count"]
    if c != 1:
        errors.append("Fail: O.query().V(\"Character:1\").inE(\"residents\").count() - %s != %s" % (c, 1))

    return errors


def test_both(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for i in O.query().V("Starship:12").both():
        if i['gid'] not in ['Character:1', 'Character:18', 'Character:19', 'Character:9', 'Film:1']:
            errors.append(
                "Fail: O.query().V(\"Starship:12\").both() - \
                Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 10:
        errors.append(
            "Fail: O.query().V(\"Starship:12\").both() %s != %d" % (count, 10))

    count = 0
    for i in O.query().V("Starship:12").both(["pilots", "starships"]):
        if i['gid'] not in ['Character:1', 'Character:18', 'Character:19', 'Character:9', 'Film:1']:
            errors.append(
                "Fail: O.query().V(\"Starship:12\").both([\"pilots\", \"starships\"]) - \
                Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 9:
        errors.append(
            "Fail: O.query().V(\"Starship:12\").both([\"pilots\", \"starships\"]) %s != %d" % (count, 9)
        )

    count = 0
    for i in O.query().E("(Film:1)-[characters]->(Character:1)").both():
        if i['gid'] not in  ["Film:1", "Character:1"]:
            errors.append(
                "Fail: O.query().E(\"(Film:1)-[characters]->(Character:1)\").both() - \
                Wrong vertex %s" % (i['gid'])
            )
        count += 1
    if count != 2:
        errors.append(
            "Fail: O.query().E(\"(Film:1)-[characters]->(Character:1)\").both() %s != %d" % (count, 2)
        )

    return errors


def test_both_edge(O, man):
    errors = []

    man.setGraph("swapi")

    c = O.query().V("Character:1").bothE().count().execute()[0]["count"]
    if c != 8:
        errors.append("Fail: O.query().V(\"Character:1\").bothE().count() %d != %d" % (c, 8))

    for i in O.query().V("Character:1").inE():
        if not (i['gid'].startswith("(Character:1)") or i['gid'].endswith("(Character:1)")):
            errors.append("Fail: O.query().V(\"Character:1\").bothE() - \
            Wrong edge %s" % (i['gid']))

    for i in O.query().V("Character:1").bothE().out():
        if i['gid'] not in ['Character:1', 'Character:1', 'Character:1', 'Character:1', 'Film:1', 'Planet:1', 'Species:1', 'Starship:12']:
            errors.append("Fail: O.query().V(\"Character:1\").bothE().out() - \
            Wrong vertex %s" % (i['gid']))

    c = O.query().V("Character:1").bothE(["homeworld", "residents"]).count().execute()[0]["count"]
    if c != 2:
        errors.append("Fail: O.query().V(\"Character:1\").inE([\"homeworld\", \"residents\"]).count() - %s != %s" % (c, 2))

    return errors


def test_limit(O, man):
    errors = []

    man.setGraph("swapi")

    tests = [
        "O.query().V().limit(3)",
        "O.query().E().limit(3)"
    ]

    expected_results = [
        ["Character:1", "Character:10", "Character:12"],
        ['(Character:1)-[films]->(Film:1)', '(Character:1)-[homeworld]->(Planet:1)', '(Character:1)-[species]->(Species:1)']
    ]

    for test, expected in zip(tests, expected_results):
        results = eval(test).execute()
        actual = [x.gid for x in results]

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


def test_skip(O, man):
    errors = []

    man.setGraph("swapi")

    tests = [
        "O.query().V().skip(3).limit(3)",
        "O.query().E().skip(3).limit(3)"
    ]

    expected_results = [
        ['Character:13', 'Character:14', 'Character:15'],
        ['(Character:1)-[starships]->(Starship:12)', '(Character:10)-[films]->(Film:1)', '(Character:10)-[species]->(Species:1)']
    ]

    for test, expected in zip(tests, expected_results):
        results = eval(test).execute()
        actual = [x.gid for x in results]

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


def test_range(O, man):
    errors = []

    man.setGraph("swapi")

    tests = [
        "O.query().V().range(3, 5)",
        "O.query().V().range(34, -1)",
        "O.query().E().range(120, 123)",
        "O.query().E().range(140, -1)"
    ]

    expected_results = [
        ['Character:13', 'Character:14'],
        ['Starship:9', 'Vehicle:4', 'Vehicle:6', 'Vehicle:7', 'Vehicle:8'],
        ['(Species:3)-[people]->(Character:13)', '(Species:4)-[films]->(Film:1)', '(Species:4)-[people]->(Character:15)'],
        ['(Vehicle:4)-[films]->(Film:1)', '(Vehicle:6)-[films]->(Film:1)', '(Vehicle:7)-[films]->(Film:1)', '(Vehicle:8)-[films]->(Film:1)']
    ]

    for test, expected in zip(tests, expected_results):
        results = eval(test).execute()
        actual = [x.gid for x in results]

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
