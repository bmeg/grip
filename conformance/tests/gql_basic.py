def test_match_by_label_and_property(man):
    man.setGraph("swapi")
    rows = man.run_gql(
        "MATCH (n:Character {name: 'Luke Skywalker'}) RETURN n.name AS name"
    )
    expected = [{"name": "Luke Skywalker"}]
    if rows != expected:
        return ["Expected %s, got %s" % (expected, rows)]
    return []


def test_relationship_traversal(man):
    man.setGraph("swapi")
    rows = man.run_gql(
        "MATCH (n:Character {name: 'Luke Skywalker'})-[:homeworld]->(planet) "
        "RETURN planet.name AS name"
    )
    expected = [{"name": "Tatooine"}]
    if rows != expected:
        return ["Expected %s, got %s" % (expected, rows)]
    return []


def test_where_or_filter(man):
    man.setGraph("swapi")
    rows = man.run_gql(
        "MATCH (n:Character) WHERE n.name = 'Luke Skywalker' OR n.name = 'C-3PO' "
        "RETURN n.name AS name"
    )
    names = sorted(row.get("name") for row in rows)
    expected = ["C-3PO", "Luke Skywalker"]
    if names != expected:
        return ["Expected names %s, got %s" % (expected, names)]
    return []


def test_no_match_returns_empty_results(man):
    man.setGraph("swapi")
    rows = man.run_gql(
        "MATCH (n:Character {name: 'No Such Character'}) RETURN n.name AS name"
    )
    if rows:
        return ["Expected no results, got %s" % rows]
    return []


def test_comparison_predicates(man):
    man.setGraph("swapi")
    cases = [
        ("n.height > 200", {"Chewbacca", "Darth Vader"}),
        ("n.height >= 228", {"Chewbacca"}),
        ("n.height < 100", {"R2-D2", "R5-D4"}),
        ("n.height <= 96", {"R2-D2"}),
    ]
    errors = []
    for predicate, expected in cases:
        rows = man.run_gql(
            "MATCH (n:Character) WHERE %s RETURN n.name AS name" % predicate
        )
        names = {row.get("name") for row in rows}
        if names != expected:
            errors.append("%s expected %s, got %s" % (predicate, expected, names))

    rows = man.run_gql(
        "MATCH (n:Character) WHERE n.name <> 'Luke Skywalker' "
        "RETURN n.name AS name"
    )
    names = {row.get("name") for row in rows}
    if len(names) != 17 or "Luke Skywalker" in names:
        errors.append("<> returned an unexpected set of names: %s" % names)
    return errors


def test_null_predicates(man):
    man.setGraph("swapi")
    null_rows = man.run_gql(
        "MATCH (n:Character) WHERE n.gender IS NULL RETURN n.name AS name"
    )
    null_names = {row.get("name") for row in null_rows}
    expected_null_names = {"C-3PO", "R2-D2", "R5-D4"}
    if null_names != expected_null_names:
        return ["Expected null-gender names %s, got %s" % (expected_null_names, null_names)]

    non_null_rows = man.run_gql(
        "MATCH (n:Character) WHERE n.gender IS NOT NULL RETURN n.name AS name"
    )
    non_null_names = {row.get("name") for row in non_null_rows}
    if len(non_null_names) != 15 or null_names.intersection(non_null_names):
        return ["IS NOT NULL returned an unexpected set of names: %s" % non_null_names]
    return []


def test_not_and_parenthesized_predicates(man):
    man.setGraph("swapi")
    rows = man.run_gql(
        "MATCH (n:Character) WHERE n.height >= 180 AND "
        "(n.name = 'Darth Vader' OR n.name = 'Chewbacca') "
        "RETURN n.name AS name"
    )
    names = {row.get("name") for row in rows}
    expected = {"Darth Vader", "Chewbacca"}
    if names != expected:
        return ["Expected names %s, got %s" % (expected, names)]

    rows = man.run_gql(
        "MATCH (n:Character) WHERE NOT (n.name = 'Luke Skywalker') "
        "RETURN n.name AS name"
    )
    names = {row.get("name") for row in rows}
    if len(names) != 17 or "Luke Skywalker" in names:
        return ["NOT returned an unexpected set of names: %s" % names]
    return []


def test_unknown_boolean_predicates(man):
    man.setGraph("swapi")
    null_names = {"C-3PO", "R2-D2", "R5-D4"}

    not_equal_rows = man.run_gql(
        "MATCH (n:Character) WHERE n.gender <> 'male' RETURN n.name AS name"
    )
    not_equal_names = {row.get("name") for row in not_equal_rows}
    if not_equal_names & null_names:
        return ["<> matched NULL gender values: %s" % (not_equal_names & null_names)]
    if "Leia Organa" not in not_equal_names or "Luke Skywalker" in not_equal_names:
        return ["<> returned an unexpected set of names: %s" % not_equal_names]

    not_rows = man.run_gql(
        "MATCH (n:Character) WHERE NOT (n.gender = 'male') RETURN n.name AS name"
    )
    not_names = {row.get("name") for row in not_rows}
    if not_names != not_equal_names:
        return ["<> and NOT equality differ: %s != %s" % (not_equal_names, not_names)]

    composed_rows = man.run_gql(
        "MATCH (n:Character) WHERE NOT (n.gender = 'male' OR n.name = 'Leia Organa') "
        "RETURN n.name AS name"
    )
    composed_names = {row.get("name") for row in composed_rows}
    if composed_names & null_names or "Leia Organa" in composed_names:
        return ["NOT over OR matched an unexpected row: %s" % composed_names]
    return []


def test_incoming_relationship_traversal(man):
    man.setGraph("swapi")
    rows = man.run_gql(
        "MATCH (planet:Planet {name: 'Tatooine'})<-[:homeworld]-(character) "
        "RETURN character.name AS name"
    )
    names = {row.get("name") for row in rows}
    expected = {
        "Beru Whitesun lars",
        "Biggs Darklighter",
        "C-3PO",
        "Darth Vader",
        "Luke Skywalker",
        "Owen Lars",
        "R5-D4",
    }
    if names != expected:
        return ["Expected names %s, got %s" % (expected, names)]
    return []


def test_undirected_relationship_traversal(man):
    man.setGraph("swapi")
    rows = man.run_gql(
        "MATCH (n:Character {name: 'Luke Skywalker'})-[:homeworld]-(planet) "
        "RETURN planet.name AS name"
    )
    expected = [{"name": "Tatooine"}]
    if rows != expected:
        return ["Expected %s, got %s" % (expected, rows)]
    return []


def test_multiple_return_items(man):
    man.setGraph("swapi")
    rows = man.run_gql(
        "MATCH (n:Character {name: 'Luke Skywalker'}) "
        "RETURN n.name AS name, n.height AS height"
    )
    expected = [{"name": "Luke Skywalker", "height": 172}]
    if rows != expected:
        return ["Expected %s, got %s" % (expected, rows)]
    return []


def test_order_by_skip_and_limit(man):
    man.setGraph("swapi")
    rows = man.run_gql(
        "MATCH (n:Character) RETURN n.name AS name "
        "ORDER BY n.name ASC SKIP 2 LIMIT 3"
    )
    names = [row.get("name") for row in rows]
    expected = ["C-3PO", "Chewbacca", "Darth Vader"]
    if names != expected:
        return ["Expected ordered names %s, got %s" % (expected, names)]
    return []