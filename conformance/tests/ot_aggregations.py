from __future__ import absolute_import

import aql
import numpy as np


def setupGraph(O):
    O.addIndex("Person", "name")
    O.addIndex("Person", "age")

    O.addVertex("1", "Person", {"name": "marko", "age": 29})
    O.addVertex("2", "Person", {"name": "vadas", "age": 25})
    O.addVertex("4", "Person", {"name": "josh", "age": 32})
    O.addVertex("6", "Person", {"name": "peter", "age": 35})
    O.addVertex("7", "Person", {"name": "marko", "age": 41})
    O.addVertex("9", "Person", {"name": "alex", "age": 30})
    O.addVertex("10", "Person", {"name": "alex", "age": 45})
    O.addVertex("11", "Person", {"name": "steve", "age": 26})
    O.addVertex("12", "Person", {"name": "alice", "age": 22})
    O.addVertex("13", "Person", {"name": "wanda", "age": 36})
    O.addVertex("5", "Software", {"name": "ripple", "lang": "java"})
    O.addVertex("3", "Software", {"name": "lop", "lang": "java"})
    O.addVertex("8", "Software", {"name": "funnel", "lang": "go"})

    O.addEdge("1", "2", "knows", {"weight": 0.5})
    O.addEdge("1", "4", "knows", {"weight": 1.0})
    O.addEdge("1", "9", "knows", {"weight": 1.0})
    O.addEdge("1", "10", "knows", {"weight": 1.0})
    O.addEdge("1", "3", "created", {"weight": 0.4})
    O.addEdge("4", "3", "created", {"weight": 0.4})
    O.addEdge("6", "3", "created", {"weight": 0.2})
    O.addEdge("4", "5", "created", {"weight": 1.0})


def test_term_aggregation(O):
    errors = []
    setupGraph(O)

    count = 0
    for row in O.aggregate(aql.term("test-agg", "Person", "name", 2)):
        count += 1
        if 'test-agg' not in row:
                errors.append("Result had Incorrect aggregation name")
                return errors
        row = row['test-agg']

        if len(row["buckets"]) != 2:
                errors.append(
                    "Unexpected number of terms: %d != %d" %
                    (len(row["buckets"]), 2)
                )

        for res in row["buckets"]:
            if res["key"] not in ["marko", "alex"]:
                errors.append(
                    "Incorrect term returned: %s" % (res["key"])
                )
            if res["value"] != 2:
                errors.append(
                    "Incorrect term count: %d != %d" %
                    (res["value"], 2))

    if count != 1:
        errors.append(
            "Incorrect number of aggregations returned: %d != %d" %
            (count, 1))

    count = 0
    for row in O.aggregate(aql.term("test-agg-no-limit", "Person", "name", size=None)):
        count += 1

        if 'test-agg-no-limit' not in row:
                errors.append("Result had Incorrect aggregation name")
                return errors
        row = row['test-agg-no-limit']

        if len(row["buckets"]) != 8:
                errors.append(
                    "Unexpected number of terms: %d != %d" %
                    (len(row["buckets"]), 8)
                )

        for res in row["buckets"]:
            if res["key"] in ["marko", "alex"]:
                if res["value"] != 2:
                    errors.append(
                        "Incorrect term count: %d != %d" %
                        (res["value"], 2))
            else:
                if res["value"] != 1:
                    errors.append(
                        "Incorrect term count: %d != %d" %
                        (res["value"], 1))

    if count != 1:
        errors.append(
            "Incorrect number of aggregations returned: %d != %d" %
            (count, 1))

    return errors


def test_traversal_term_aggregation(O):
    errors = []
    setupGraph(O)

    count = 0
    for row in O.query().V("1").out().aggregate(aql.term("traversal-agg", "Person", "name")):
        if 'traversal-agg' not in row:
            errors.append("Result had Incorrect aggregation name")
            return errors
        row = row['traversal-agg']

        count += 1
        if len(row["buckets"]) != 3:
                errors.append(
                    "Unexpected number of terms: %d != %d" %
                    (len(row["buckets"]), 3)
                )

        for res in row["buckets"]:
            if res["key"] == "alex":
                if res["value"] != 2:
                    errors.append(
                        "Incorrect term count: %d != %d" %
                        (res["value"], 2))
            else:
                if res["value"] != 1:
                    errors.append(
                        "Incorrect term count: %d != %d" %
                        (res["value"], 1))

    if count != 1:
        errors.append(
            "Incorrect number of aggregations returned: %d != %d" %
            (count, 1))

    return errors


def test_histogram_aggregation(O):
    errors = []
    setupGraph(O)

    count = 0
    for row in O.aggregate(aql.histogram("test-agg", "Person", "age", 5)):
        count += 1

        if 'test-agg' not in row:
            errors.append("Result had Incorrect aggregation name")
            return errors
        row = row['test-agg']

        if len(row["buckets"]) != 6:
                errors.append(
                    "Unexpected number of terms: %d != %d" %
                    (len(row["buckets"]), 6)
                )

        for res in row["buckets"]:
            if res["key"] == 20:
                if res["value"] != 1:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 25:
                if res["value"] != 3:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 30:
                if res["value"] != 2:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 35:
                if res["value"] != 2:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 40:
                if res["value"] != 1:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 45:
                if res["value"] != 1:
                    errors.append("Incorrect bucket count returned: %s" % res)
            else:
                errors.append("Incorrect bucket key returned: %s" % res)

    if count != 1:
        errors.append(
            "Incorrect number of aggregations returned: %d != %d" %
            (count, 1))

    return errors


def test_traversal_histogram_aggregation(O):
    errors = []
    setupGraph(O)

    count = 0
    for row in O.query().V("1").out().aggregate(aql.histogram("traversal-agg", "Person", "age", 5)):
        count += 1
        
        if 'traversal-agg' not in row:
            errors.append("Result had Incorrect aggregation name")
            return errors
        row = row['traversal-agg']

        if len(row["buckets"]) != 5:
                errors.append(
                    "Unexpected number of terms: %d != %d" %
                    (len(row["buckets"]), 5)
                )

        for res in row["buckets"]:
            if res["key"] == 25:
                if res["value"] != 1:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 30:
                if res["value"] != 2:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 35:
                if res["value"] != 0:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 40:
                if res["value"] != 0:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 45:
                if res["value"] != 1:
                    errors.append("Incorrect bucket count returned: %s" % res)
            else:
                errors.append("Incorrect bucket key returned: %s" % res)

    if count != 1:
        errors.append(
            "Incorrect number of aggregations returned: %d != %d" %
            (count, 1))

    return errors


def test_percentile_aggregation(O):
    errors = []
    setupGraph(O)

    count = 0
    percents = [1, 5, 25, 50, 75, 95, 99, 99.9]
    for row in O.aggregate(aql.percentile("test-agg", "Person", "age", percents)):
        count += 1

        if 'test-agg' not in row:
            errors.append("Result had Incorrect aggregation name")
            return errors
        row = row['test-agg']

        if len(row["buckets"]) != len(percents):
            errors.append(
                "Unexpected number of terms: %d != %d" %
                (len(row["buckets"]), len(percents))
            )

        ages = np.array([29, 41, 25, 32, 35, 30, 45, 26, 22, 36])

        # for tests quantiles need to be withing 10% of the actual value
        def getMinMax(input_data, percent, accuracy=0.1):
            return np.percentile(input_data, percent) * (1 - accuracy), np.percentile(input_data, percent) * (1 + accuracy)

        for res in row["buckets"]:
            if res["key"] == 1:
                minpv, maxpv = getMinMax(ages, res["key"])
                if res["value"] <= minpv or res["value"] >= maxpv:
                    errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
            elif res["key"] == 5:
                minpv, maxpv = getMinMax(ages, res["key"])
                if res["value"] <= minpv or res["value"] >= maxpv:
                    errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
            elif res["key"] == 25:
                minpv, maxpv = getMinMax(ages, res["key"])
                if res["value"] <= minpv or res["value"] >= maxpv:
                    errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
            elif res["key"] == 50:
                minpv, maxpv = getMinMax(ages, res["key"])
                if res["value"] <= minpv or res["value"] >= maxpv:
                    errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
            elif res["key"] == 75:
                minpv, maxpv = getMinMax(ages, res["key"])
                if res["value"] <= minpv or res["value"] >= maxpv:
                    errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
            elif res["key"] == 95:
                minpv, maxpv = getMinMax(ages, res["key"])
                if res["value"] <= minpv or res["value"] >= maxpv:
                    errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
            elif res["key"] == 99:
                minpv, maxpv = getMinMax(ages, res["key"])
                if res["value"] <= minpv or res["value"] >= maxpv:
                    errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
            elif res["key"] == 99.9:
                minpv, maxpv = getMinMax(ages, res["key"])
                if res["value"] <= minpv or res["value"] >= maxpv:
                    errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
            else:
                errors.append("Incorrect bucket key returned: %s" % res)

    if count != 1:
        errors.append(
            "Incorrect number of aggregations returned: %d != %d" %
            (count, 1))

    return errors


def test_traversal_percentile_aggregation(O):
    errors = []
    setupGraph(O)

    count = 0
    percents = [1, 5, 25, 50, 75, 95, 99, 99.9]
    for row in O.query().V("1").out().aggregate(aql.percentile("traversal-agg", "Person", "age", percents)):
        count += 1

        if 'traversal-agg' not in row:
            errors.append("Result had Incorrect aggregation name")
            return errors
        row = row['traversal-agg']

        if len(row["buckets"]) != len(percents):
            errors.append(
                "Unexpected number of terms: %d != %d" %
                (len(row["buckets"]), len(percents))
            )

        ages = np.array([25, 32, 30, 45])

        # for tests quantiles need to be withing 15% of the actual value
        def getMinMax(input_data, percent, accuracy=0.15):
            return np.percentile(input_data, percent) * (1 - accuracy), np.percentile(input_data, percent) * (1 + accuracy)

        for res in row["buckets"]:
            if res["key"] == 1:
                minpv, maxpv = getMinMax(ages, res["key"])
                if res["value"] <= minpv or res["value"] >= maxpv:
                    errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
            elif res["key"] == 5:
                minpv, maxpv = getMinMax(ages, res["key"])
                if res["value"] <= minpv or res["value"] >= maxpv:
                    errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
            elif res["key"] == 25:
                minpv, maxpv = getMinMax(ages, res["key"])
                if res["value"] <= minpv or res["value"] >= maxpv:
                    errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
            elif res["key"] == 50:
                minpv, maxpv = getMinMax(ages, res["key"])
                if res["value"] <= minpv or res["value"] >= maxpv:
                    errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
            elif res["key"] == 75:
                minpv, maxpv = getMinMax(ages, res["key"])
                if res["value"] <= minpv or res["value"] >= maxpv:
                    errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
            elif res["key"] == 95:
                minpv, maxpv = getMinMax(ages, res["key"])
                if res["value"] <= minpv or res["value"] >= maxpv:
                    errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
            elif res["key"] == 99:
                minpv, maxpv = getMinMax(ages, res["key"])
                if res["value"] <= minpv or res["value"] >= maxpv:
                    errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
            elif res["key"] == 99.9:
                minpv, maxpv = getMinMax(ages, res["key"])
                if res["value"] <= minpv or res["value"] >= maxpv:
                    errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
            else:
                errors.append("Incorrect bucket key returned: %s" % res)

    if count != 1:
        errors.append(
            "Incorrect number of aggregations returned: %d != %d" %
            (count, 1))

    return errors
