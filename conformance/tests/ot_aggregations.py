from __future__ import absolute_import

import gripql
import numpy as np


def test_simple(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for row in O.query().V().aggregate(gripql.term("simple-agg", "name")):
        if 'simple-agg' not in row:
            errors.append("Result had Incorrect aggregation name")
            return errors
        row = row['simple-agg']
        for res in row["buckets"]:
            count += 1
            if res['key'] in ['marko', 'alex']:
                if res['value'] != 2:
                    errors.append("Wrong key count for %s" % (res['key']))
            elif res['key'] in ['funnel', 'josh', 'vadas', 'peter', 'steve', 'lop', 'alice', 'wanda', 'ripple']:
                if res['value'] != 1:
                    errors.append("Wrong key count for %s" % (res['key']))
    if count != 23:
        errors.append("Wrong number of results recieved %d" % (count))
    return errors


def test_traversal_term_aggregation(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for row in O.query().V("01").out().hasLabel("Person").aggregate(gripql.term("traversal-agg", "name")):
        if 'traversal-agg' not in row:
            errors.append("Result had Incorrect aggregation name")
            return errors
        row = row['traversal-agg']

        count += 1
        if len(row["buckets"]) != 5:
            errors.append(
                "Unexpected number of terms: %d != %d" %
                (len(row["buckets"]), 5)
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


def test_traversal_histogram_aggregation(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for row in O.query().V("01").out().hasLabel("Person").aggregate(gripql.histogram("traversal-agg", "age", 5)):
        count += 1
        if 'traversal-agg' not in row:
            errors.append("Result had Incorrect aggregation name")
            return errors
        row = row['traversal-agg']

        if len(row["buckets"]) < 4:
            errors.append(
                "Unexpected number of terms: %d != %d" %
                (len(row["buckets"]), 4)
            )

        for res in row["buckets"]:
            if res["key"] == 20:
                if res["value"] != 1:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 25:
                if res["value"] != 2:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 30:
                if res["value"] != 1:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 35:
                if res["value"] != 1:
                    errors.append("Incorrect bucket count returned: %s" % res)
            else:
                errors.append("Incorrect bucket key returned: %s" % res)

        if len(row["buckets"]) != 4:
            errors.append("Incorrect bucket size returned: %d" % len(row["buckets"]))

    if count != 1:
        errors.append(
            "Incorrect number of aggregations returned: %d != %d" %
            (count, 1))

    return errors


def test_traversal_percentile_aggregation(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    percents = [1, 5, 25, 50, 75, 95, 99, 99.9]
    for row in O.query().V("01").out().hasLabel("Person").aggregate(gripql.percentile("traversal-agg", "age", percents)):
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

        ages = np.array([25, 35, 32, 26, 22])

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


def test_traversal_edge_histogram_aggregation(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for row in O.query().V().hasLabel("Person").outE().aggregate(gripql.histogram("edge-agg", "count", 4)):
        count += 1
        if 'edge-agg' not in row:
            errors.append("Result had Incorrect aggregation name")
            return errors
        row = row['edge-agg']

        if len(row["buckets"]) < 2:
            errors.append(
                "Unexpected number of terms: %d != %d" %
                (len(row["buckets"]), 2)
            )

        for res in row["buckets"]:
            if res["key"] == 4:
                if res["value"] != 2:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 20:
                if res["value"] != 2:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 28:
                if res["value"] != 1:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 32:
                if res["value"] != 2:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 48:
                if res["value"] != 1:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 72:
                if res["value"] != 3:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == 88:
                if res["value"] != 1:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] in [8, 12, 16, 24, 36, 40, 44, 52, 56, 60, 64, 68, 76, 80, 84]:
                if res["value"] != 0:
                    errors.append("Incorrect bucket count returned: %s" % res)
            else:
                errors.append("Incorrect bucket key returned: %s" % res)

        if len(row["buckets"]) != 22:
            errors.append("Incorrect bucket count: %d" % len(row["buckets"]))
    if count != 1:
        errors.append(
            "Incorrect number of aggregations returned: %d != %d" %
            (count, 1))

    return errors


def test_traversal_gid_aggregation(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for row in O.query().V().hasLabel("Person").as_("a").out("knows").select("a").aggregate(gripql.term("gid-agg", "_gid")):
        count += 1
        if 'gid-agg' not in row:
            errors.append("Result had Incorrect aggregation name")
            return errors
        row = row['gid-agg']

        if len(row["buckets"]) < 2:
            errors.append(
                "Unexpected number of terms: %d != %d" %
                (len(row["buckets"]), 2)
            )

        for res in row["buckets"]:
            if res["key"] == "01":
                if res["value"] != 4:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == "02":
                if res["value"] != 1:
                    errors.append("Incorrect bucket count returned: %s" % res)
            elif res["key"] == "04":
                if res["value"] != 1:
                    errors.append("Incorrect bucket count returned: %s" % res)
            else:
                errors.append("Incorrect bucket key returned: %s" % res)

    if count != 1:
        errors.append(
            "Incorrect number of aggregations returned: %d != %d" %
            (count, 1))
    return errors
