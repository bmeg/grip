from __future__ import absolute_import


def test_render(O):
    errors = []

    O.addVertex("1", "Person", {"name": "marko", "age": "29"})
    O.addVertex("2", "Person", {"name": "vadas", "age": "27"})
    O.addVertex("3", "Software", {"name": "lop", "lang": "java"})
    O.addVertex("4", "Person", {"name": "josh", "age": "32"})
    O.addVertex("5", "Software", {"name": "ripple", "lang": "java"})
    O.addVertex("6", "Person", {"name": "peter", "age": "35"})

    O.addEdge("1", "3", "created", {"weight": 0.4})
    O.addEdge("1", "2", "knows", {"weight": 0.5})
    O.addEdge("1", "4", "knows", {"weight": 1.0})
    O.addEdge("4", "3", "created", {"weight": 0.4})
    O.addEdge("6", "3", "created", {"weight": 0.2})
    O.addEdge("4", "5", "created", {"weight": 1.0})

    query = O.query().V().hasLabel("Person").render(
        {
            "Name": "name",
            "Age": "age"
        }
    )
    for row in query:
        if 'Age' not in row or "Name" not in row:
            errors.append("Missing fields")

    query = O.query().V().hasLabel("Person").render(
        {
            "Name": "name",
            "NonExistent": "non-existent"
        }
    )
    for row in query:
        if 'NonExistent' not in row or "Name" not in row:
            errors.append("Missing fields")

    query = O.query().V().hasLabel("Person").render(["name", "age"])
    for row in query:
        if not isinstance(row, list):
            errors.append("unexpected output format")
        if len(row) != 2:
            errors.append("Missing fields")

    query = O.query().V().hasLabel("Person").render(["name", "non-existent"])
    for row in query:
        if not isinstance(row, list):
            errors.append("unexpected output format")
        if len(row) != 2:
            errors.append("Missing fields")

    return errors
