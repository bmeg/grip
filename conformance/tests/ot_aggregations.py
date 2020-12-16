from __future__ import absolute_import

import gripql
import numpy as np

eye_color_count_map = {
    "brown": 4,
    "blue": 6,
    "red": 2,
    "yellow": 2,
    "black": 1,
    "blue-gray": 1,
    "hazel": 1,
    "orange": 1
}


def test_simple(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for row in G.query().V().aggregate(gripql.term("simple-agg", "eye_color")):
        if 'simple-agg' not in row:
            errors.append("Result had Incorrect aggregation name")
            return errors
        row = row['simple-agg']
        for res in row["buckets"]:
            count += 1
            if eye_color_count_map[res['key']] != res['value']:
                errors.append("Wrong key count for %s %d != %d" % (res['key'], res["value"], eye_color_count_map[res['key']]))
    if count != 8:
        errors.append("Wrong number of results recieved %d" % (count))
    return errors


def test_traversal_term_aggregation(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for row in G.query().V("Film:1").out().hasLabel("Character").aggregate(gripql.term("traversal-agg", "eye_color")):
        if 'traversal-agg' not in row:
            errors.append("Result had Incorrect aggregation name")
            return errors
        row = row['traversal-agg']

        count += 1
        if len(row["buckets"]) != len(eye_color_count_map):
            errors.append(
                "Unexpected number of terms: %d != %d" %
                (len(row["buckets"]), len(eye_color_count_map))
            )

        for res in row["buckets"]:
            if eye_color_count_map[res['key']] != res['value']:
                errors.append("Wrong key count for %s %d != %d" % (res['key'], res["value"], eye_color_count_map[res['key']]))

    if count != 1:
        errors.append(
            "Incorrect number of aggregations returned: %d != %d" %
            (count, 1))

    return errors


def test_traversal_histogram_aggregation(man):
    errors = []

    G = man.setGraph("swapi")

    height_agg_map = {
        75: 2,
        100: 0,
        125: 0,
        150: 6,
        175: 8,
        200: 1,
        225: 1
    }

    count = 0
    for row in G.query().V("Film:1").out().hasLabel("Character").aggregate(gripql.histogram("traversal-agg", "height", 25)):
        count += 1
        if 'traversal-agg' not in row:
            errors.append("Result had Incorrect aggregation name")
            return errors
        row = row['traversal-agg']

        for res in row["buckets"]:
            print(res)
            if height_agg_map[res["key"]] != res["value"]:
                errors.append("Incorrect bucket count returned: %s" % res)

        if len(row["buckets"]) != 7:
            errors.append("Incorrect bucket size returned: %d" % len(row["buckets"]))

    if count != 1:
        errors.append(
            "Incorrect number of aggregations returned: %d != %d" %
            (count, 1))

    return errors


def test_traversal_percentile_aggregation(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    percents = [1, 5, 25, 50, 75, 95, 99, 99.9]
    for row in G.query().V("Film:1").out().hasLabel("Character").aggregate(gripql.percentile("traversal-agg", "height", percents)):
        count += 1

        if 'traversal-agg' not in row:
            errors.append("Result had Incorrect aggregation name")
            return errors
        row = row['traversal-agg']
        print(row)
        if len(row["buckets"]) != len(percents):
            errors.append(
                "Unexpected number of terms: %d != %d" %
                (len(row["buckets"]), len(percents))
            )

        heights = np.array([96, 97, 150, 165, 167, 170, 172, 173, 175, 178, 180, 180, 180, 182, 183, 188, 202, 228])

        # for tests quantiles need to be withing 15% of the actual value
        def getMinMax(input_data, percent, accuracy=0.15):
            return np.percentile(input_data, percent) * (1 - accuracy), np.percentile(input_data, percent) * (1 + accuracy)

        for res in row["buckets"]:
            if res["key"] in percents:
                minpv, maxpv = getMinMax(heights, res["key"])
                if res["value"] <= minpv or res["value"] >= maxpv:
                    errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
            else:
                errors.append("Incorrect bucket key returned: %s" % res)

    if count != 1:
        errors.append(
            "Incorrect number of aggregations returned: %d != %d" %
            (count, 1))

    return errors


def test_traversal_edge_histogram_aggregation(man):
    errors = []

    G = man.setGraph("swapi")

    scene_count_agg_map = {
        8: 1,
        12: 1,
        16: 0,
        20: 1
    }

    count = 0
    for row in G.query().V().hasLabel("Film").outE().aggregate(gripql.histogram("edge-agg", "scene_count", 4)):
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
            if scene_count_agg_map[res["key"]] != res["value"]:
                errors.append("Incorrect bucket count returned: %s" % res)

        if len(row["buckets"]) != len(scene_count_agg_map):
            errors.append("Incorrect bucket count: %d" % len(row["buckets"]))
    if count != 1:
        errors.append(
            "Incorrect number of aggregations returned: %d != %d" %
            (count, 1))

    return errors


def test_traversal_gid_aggregation(man):
    errors = []

    G = man.setGraph("swapi")

    planet_agg_map = {
        "Planet:1": 7,
        "Planet:2": 2
    }

    count = 0
    for row in G.query().V().hasLabel("Planet").as_("a").out("residents").select("a").aggregate(gripql.term("gid-agg", "_gid")):
        count += 1
        if 'gid-agg' not in row:
            errors.append("Result had Incorrect aggregation name")
            return errors
        row = row['gid-agg']
        print(row)

        if len(row["buckets"]) < len(planet_agg_map):
            errors.append(
                "Unexpected number of terms: %d != %d" %
                (len(row["buckets"]), 2)
            )

        for res in row["buckets"]:
            if planet_agg_map[res["key"]] != res["value"]:
                errors.append("Incorrect bucket count returned: %s" % res)

    if count != 1:
        errors.append(
            "Incorrect number of aggregations returned: %d != %d" %
            (count, 1))
    return errors
