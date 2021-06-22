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
        if row['name'] != 'simple-agg':
            errors.append("Result had Incorrect aggregation name")
            return errors
        count += 1
        if eye_color_count_map[row['key']] != row['value']:
                errors.append("Wrong key count for %s %d != %d" % (row['key'], row["value"], eye_color_count_map[row['key']]))
    if count != 8:
        errors.append("Wrong number of results recieved %d" % (count))
    return errors


def test_traversal_term_aggregation(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for row in G.query().V("Film:1").out().hasLabel("Character").aggregate(gripql.term("traversal-agg", "eye_color")):
        if row['name'] != 'traversal-agg':
            errors.append("Result had Incorrect aggregation name")
            return errors

        count += 1

        if eye_color_count_map[row['key']] != row['value']:
            errors.append("Wrong key count for %s %d != %d" % (row['key'], row["value"], eye_color_count_map[row['key']]))

    if count != len(eye_color_count_map):
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
        if row['name'] != 'traversal-agg':
            errors.append("Result had Incorrect aggregation name")
            return errors

        if height_agg_map[row["key"]] != row["value"]:
            errors.append("Incorrect bucket count returned: %s" % row)

    if count != 7:
        errors.append("Incorrect bucket size returned: %d" % count)

    return errors


def test_traversal_percentile_aggregation(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    percents = [1, 5, 25, 50, 75, 95, 99, 99.9]
    heights = np.array([96, 97, 150, 165, 167, 170, 172, 173, 175, 178, 180, 180, 180, 182, 183, 188, 202, 228])

    data = []
    for row in G.query().V("Film:1").out().hasLabel("Character").aggregate(gripql.percentile("traversal-agg", "height", percents)):
        count += 1
        if row['name'] != 'traversal-agg':
            errors.append("Result had Incorrect aggregation name")
            return errors
        data.append(row)



    # for tests quantiles need to be withing 15% of the actual value
    def getMinMax(input_data, percent, accuracy=0.15):
        return np.percentile(input_data, percent) * (1 - accuracy), np.percentile(input_data, percent) * (1 + accuracy)

    for res in data:
        if res["key"] in percents:
            minpv, maxpv = getMinMax(heights, res["key"])
            if res["value"] <= minpv or res["value"] >= maxpv:
                errors.append("Incorrect quantile value returned for %.2f:\n\tmin: %.2f\n\tmax: %.2f\n\tactual: %.2f" % (res["key"], minpv, maxpv, res["value"]))
        else:
            errors.append("Incorrect bucket key returned: %s" % res)

    if count != len(percents):
        errors.append(
            "Unexpected number of terms: %d != %d" %
            (len(row["buckets"]), len(percents))
        )

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
        if row['name'] != 'edge-agg':
            errors.append("Result had Incorrect aggregation name")
            return errors

        if scene_count_agg_map[row["key"]] != row["value"]:
            errors.append("Incorrect bucket count returned: %s" % row)


    if count < 2:
        errors.append(
            "Unexpected number of terms: %d != %d" %
            (len(row["buckets"]), 2)
        )

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
        if 'gid-agg' != row['name']:
            errors.append("Result had Incorrect aggregation name")
            return errors

        if planet_agg_map[row["key"]] != row["value"]:
            errors.append("Incorrect bucket count returned: %s" % res)

    if count != 2:
        errors.append(
            "Incorrect number of aggregations returned: %d != %d" %
            (count, 2))
    return errors


def test_field_aggregation(man):
    errors = []

    fields = [ 'orbital_period', 'gravity', 'terrain', 'name','climate', 'system', 'diameter', 'rotation_period', 'url', 'population', 'surface_water']

    G = man.setGraph("swapi")
    count = 0
    for row in G.query().V().hasLabel("Planet").aggregate(gripql.field("gid-agg", "$._data")):
        if row["key"] not in fields:
            errors.append("unknown field returned: %s" % (row['key']))
        if row["value"] != 3:
            errors.append("incorrect count returned: %s" % (row['value']))
        count += 1
    if count != 11:
        errors.append("Incorrect number of results returned")
    return errors


def test_field_type_aggregation(man):
    errors = []

    types = {
        "population" : 'NUMERIC',
        "name" : "STRING",
        "diameter" : "NUMERIC",
        "gravity" : "UNKNOWN"
    }
    G = man.setGraph("swapi")
    count = 0
    for row in G.query().V().hasLabel("Planet").aggregate(list( gripql.type(a) for a in ["population", "name", "gravity", "diameter"])):
        if types[row['name']] != row['key']:
            errors.append("Wrong type: %s != %s" % (types[row['name']], row['key']))
        count += 1
    if count != 4:
        errors.append("Incorrect number of results returned")
    return errors
