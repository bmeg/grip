from __future__ import absolute_import

import gripql
from gripql import __


def test_match(O, man):
    """
    http://tinkerpop.apache.org/docs/3.3.4/reference/#match-step

    First example
    """
    errors = []

    """
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
    """

    man.setGraph("graph1")

    query = O.query().V().match([
        __.as_('a').in_('parent').as_('b'),
        __.as_('b').has(gripql.eq('name', 'vadas')),
        __.as_('b').out('likes').as_('c'),
        __.as_('c').has(gripql.eq('lang', "english"))
    ]).select(['a', 'c'])

    count = 0
    for row in query.execute(stream=True):
        count += 1
        if len(row) != 2:
            errors.append("Incorrect number of marks returned in row")
            continue
        if row["a"]['data']['name'] not in ["marko"]:
            errors.append("Incorrect 'a' return")
        if row["c"]['data']['name'] != "The Joy of Cooking":
            errors.append("Incorrect 'c' return")

    if count != 1:
        errors.append("Incorrect return count: %d != %d" % (count, 1))

    return errors


# Below is an example of the type of match query that is not currently supported
# def test_match_nonlinear(O):
#     """
#     http://tinkerpop.apache.org/docs/3.3.4/reference/#match-step
#     Second example
#     """
#     errors = []

#     O.addVertex("340", "Artist", {"name": "Garcia"})
#     O.addVertex("351", "Artist", {"name": "Weir"})
#     O.addVertex("354", "Artist", {"name": "Lesh"})
#     O.addVertex("108", "Song", {"name": "Duprees Diamond Blues"})
#     O.addVertex("278", "Song", {"name": "Cryptical Envelopment"})
#     O.addVertex("355", "Song", {"name": "Cant Come Down"})
#     O.addVertex("361", "Song", {"name": "Cream Puff War"})
#     O.addVertex("371", "Song", {"name": "Equinox"})
#     O.addVertex("392", "Song", {"name": "Little Star"})
#     O.addVertex("46", "Song", {"name": "Box Of Rain"})
#     O.addVertex("85", "Song", {"name": "Estimated Prophet"})

#     O.addEdge("278", "340", "writtenBy")
#     O.addEdge("355", "340", "writtenBy")
#     O.addEdge("361", "340", "writtenBy")
#     O.addEdge("108", "340", "sungBy")
#     O.addEdge("278", "340", "sungBy")
#     O.addEdge("361", "340", "sungBy")
#     O.addEdge("371", "354", "writtenBy")
#     O.addEdge("46", "354", "sungBy")
#     O.addEdge("392", "351", "writtenBy")
#     O.addEdge("85", "351", "sungBy")

#     query = O.query().V().match([
#         __.as_('a').has(gripql.eq('name', 'Garcia')),
#         __.as_('a').in_('writtenBy').as_('b'),
#         __.as_('a').in_('sungBy').as_('b')
#     ]).select(['b'])

#     count = 0
#     for row in query.execute(stream=True):
#         count += 1
#         print(row)
#         if row['data']['name'] not in ["Cream Puff War", "Cryptical Envelopment"]:
#             errors.append("Incorrect return")

#     if count != 2:
#         errors.append("Incorrect return count: %d != %d" % (count, 2))

#     return errors
