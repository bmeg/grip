from __future__ import absolute_import

import aql


def test_term_aggregation(O):
    errors = []

    O.addIndex("Person", "name")

    O.addVertex("1", "Person", {"name": "marko", "age": "29"})
    O.addVertex("2", "Person", {"name": "vadas", "age": "27"})
    O.addVertex("3", "Software", {"name": "lop", "lang": "java"})
    O.addVertex("4", "Person", {"name": "josh", "age": "32"})
    O.addVertex("5", "Software", {"name": "ripple", "lang": "java"})
    O.addVertex("6", "Person", {"name": "peter", "age": "35"})
    O.addVertex("7", "Person", {"name": "marko", "age": "35"})

    O.addEdge("1", "3", "created", {"weight": 0.4})
    O.addEdge("1", "2", "knows", {"weight": 0.5})
    O.addEdge("1", "4", "knows", {"weight": 1.0})
    O.addEdge("4", "3", "created", {"weight": 0.4})
    O.addEdge("6", "3", "created", {"weight": 0.2})
    O.addEdge("4", "5", "created", {"weight": 1.0})

    count = 0
    for row in O.aggregate(aql.term("test-agg", "Person", "name", 2)):
        count += 1
        if len(row["buckets"]) != 2:
                errors.append(
                    "Number of terms differs from requested size: %d != %d" %
                    (len(row["buckets"]), 2)
                )

        if row['name'] != 'test-agg':
                errors.append("Result had Incorrect aggregation name")

        for res in row["buckets"]:
            if res["key"] == "marko":
                if res["value"] != 2:
                    errors.append(
                        "Incorrect term count: %d != %d" %
                        (res["value"], 2))
            else:
                if res["value"] != 1:
                    errors.append(
                        "Incorrect term count: %d != %d" %
                        (row['count'], 1))

    if count != 1:
        errors.append(
            "Incorrect number of aggregations returned: %d != %d" %
            (count, 1))

    return errors


# def test_percentile_aggregation(O):
#     errors = []
#     return errors


# def test_histogram_aggregation(O):
#     errors = []
#     return errors
