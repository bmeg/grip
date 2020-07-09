from __future__ import absolute_import

import gripql
from gripql import __


def test_match(G, man):
    """
    http://tinkerpop.apache.org/docs/3.3.4/reference/#match-step

    First example
    """
    errors = []

    man.setGraph("swapi")

    query = G.query().V().match([
        __.as_('a').in_('residents').as_('b'),
        __.as_('b').has(gripql.eq('name', 'Tatooine')),
        __.as_('b').out('films').as_('c'),
        __.as_('c').has(gripql.eq('title', "A New Hope"))
    ]).select(['a', 'c'])

    count = 0
    for row in query.execute(stream=True):
        count += 1
        if len(row) != 2:
            errors.append("Incorrect number of marks returned in row")
            continue
        if row["a"]['data']['name'] not in ["Luke Skywalker", "Biggs Darklighter", "R5-D4", "Beru Whitesun lars", "Owen Lars", "Darth Vader", "C-3PO"]:
            errors.append("Incorrect 'a' return")
        if row["c"]['data']['title'] != "A New Hope":
            errors.append("Incorrect 'c' return")

    if count != 7:
        errors.append("Incorrect return count: %d != %d" % (count, 7))

    return errors
