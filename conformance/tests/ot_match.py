from __future__ import absolute_import

import gripql


def test_match_count(O):
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

    query = O.query().V().match([
        O.query().mark('a').out('created').mark('b'),
        O.query().mark('b').where(gripql.eq('$.name', 'lop')),
        O.query().mark('b').in_('created').mark('c'),
        O.query().mark('c').where(gripql.eq('$.age', "29"))
    ]).select(['a', 'c'])

    count = 0
    for row in query.execute(stream=True):
        count += 1
        if len(row) != 2:
            errors.append("Incorrect number of marks returned in row")
            continue
        if row["c"]['data']['name'] != "marko":
            errors.append("Incorrect return")

    if count != 3:
        errors.append("Incorrect return count: %d != %d" % (count, 3))

    return errors
