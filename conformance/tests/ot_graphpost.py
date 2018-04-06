

def test_subgraph_post(O):
    errors = []

    graph = {
        "vertices": [
            {"gid": "1", "label": "Person", "data": {"name": "marko", "age": "29"}},
            {"gid": "2", "label": "Person", "data": {"name": "vadas", "age": "27"}},
            {"gid": "3", "label": "Software", "data": {"name": "lop", "lang": "java"}},
            {"gid": "4", "label": "Person", "data": {"name": "josh", "age": "32"}},
            {"gid": "5", "label": "Software", "data": {"name": "ripple", "lang": "java"}},
            {"gid": "6", "label": "Person", "data": {"name": "peter", "age": "35"}},
        ],
        "edges": [
            {"from": "1", "to": "3", "label": "created", "data": {"weight": 0.4}},
            {"from": "1", "to": "2", "label": "knows", "data": {"weight": 0.5}},
            {"from": "1", "to": "4", "label": "knows", "data": {"weight": 1.0}},
            {"from": "4", "to": "3", "label": "created", "data": {"weight": 0.4}},
            {"from": "6", "to": "3", "label": "created", "data": {"weight": 0.2}},
            {"from": "4", "to": "5", "label": "created", "data": {"weight": 1.0}}
        ]
    }

    O.addSubGraph(graph)

    query = O.query().V().match([
        O.mark('a').outgoing('created').mark('b'),
        O.mark('b').has('name', 'lop'),
        O.mark('b').incoming('created').mark('c'),
        O.mark('c').has('age', "29")
    ]).select(['a', 'c'])  # .by('name')

    count = 0
    for row in query.execute():
        count += 1
        if row[1]['vertex']['data']['name'] != "marko":
            errors.append("Incorrect return")

    count = 0
    for row in O.query().V():
        count += 1
    if count != 6:
        errors.append("Found %s rows, not 6" % count)

    return errors
