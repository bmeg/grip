from __future__ import absolute_import

import aql


def test_where(O):
    errors = []

    O.addVertex("vertex1", "person", {"name": "han", "occupation": "smuggler"})
    O.addVertex("vertex2", "person", {"name": "luke", "occupation": "jedi"})
    O.addVertex("vertex3", "robot", {"name": "r2-d2"})
    O.addVertex("vertex4", "robot", {"name": "c-3po"})
    O.addVertex("vertex5", "person", {"name": "obi-wan", "occupation": "jedi"})
    O.addVertex("vertex6", "person", {"name": "vader", "occupation": "sith"})

    O.addEdge("vertex1", "vertex2", "friend", id="edge1")
    O.addEdge("vertex2", "vertex3", "owner", id="edge2")
    O.addEdge("vertex2", "vertex4", "friend", id="edge3")

    count = 0
    for i in O.query().V().where(aql.eq("$.occupation", "jedi")).execute():
        count += 1
        if i['vertex']['gid'] not in ["vertex2", "vertex5"]:
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 2:
        errors.append(
            "Fail: O.query().V().where(aql.eq(\"$.occupation\", \"jedi\")) %s != %s" %
            (count, 2))

    count = 0
    for i in O.query().V().where(aql.and_(
            aql.eq("$.label", "person"),
            aql.not_(aql.or_(aql.eq("$.occupation", "jedi"),
                             aql.eq("$.occupation", "sith")))
    )).execute():
        count += 1
        if i['vertex']['gid'] != "vertex1":
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 1:
        errors.append(
            "Fail: O.query().V().where(aql.and_(aql.eq(\"$.label\", \"person\"), aql.not_(aql.eq(\"$.occupation\", \"jedi\")))) %s != %s" %
            (count, 1)
        )

    return errors


def test_where_label(O):
    errors = []

    O.addVertex("vertex1", "person", {"name": "han", "occupation": "smuggler"})
    O.addVertex("vertex2", "person", {"name": "luke", "occupation": "jedi"})
    O.addVertex("vertex3", "robot", {"name": "r2-d2"})
    O.addVertex("vertex4", "robot", {"name": "c-3po"})
    O.addVertex("vertex5", "person", {"name": "obi-wan", "occupation": "jedi"})

    O.addEdge("vertex1", "vertex2", "friend", id="edge1")
    O.addEdge("vertex2", "vertex3", "owner", id="edge2")
    O.addEdge("vertex2", "vertex4", "friend", id="edge3")

    count = 0
    for i in O.query().V().where(aql.eq("$.label", "person")).execute():
        count += 1
        if i['vertex']['label'] != "person":
            errors.append("Wrong vertex label %s" % (i['vertex']['label']))
    if count != 3:
        errors.append(
            "Fail: O.query().V().where(aql.eq(\"$.label\", \"person\")) %s != %s" %
            (count, 3))

    return errors


def test_where_id(O):
    errors = []

    O.addVertex("vertex1", "person", {"name": "han", "occupation": "smuggler"})
    O.addVertex("vertex2", "person", {"name": "luke", "occupation": "jedi"})
    O.addVertex("vertex3", "robot", {"name": "r2-d2"})
    O.addVertex("vertex4", "robot", {"name": "c-3po"})
    O.addVertex("vertex5", "person", {"name": "obi-wan", "occupation": "jedi"})

    O.addEdge("vertex1", "vertex2", "friend", id="edge1")
    O.addEdge("vertex2", "vertex3", "owner", id="edge2")
    O.addEdge("vertex2", "vertex4", "friend", id="edge3")

    count = 0
    for i in O.query().V().where(aql.eq("$.gid", "vertex3")).execute():
        count += 1
        if i['vertex']['gid'] != "vertex3":
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 1:
        errors.append(
            "Fail: O.query().V().where(aql.eq(\"$.gid\", \"vertex3\")) %s != %s" %
            (count, 1))

    return errors
