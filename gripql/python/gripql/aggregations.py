from __future__ import absolute_import, print_function, unicode_literals


def term(name, field, size=None):
    agg = {
        "name": name,
        "term": {"field": field}
    }
    if size:
        agg["term"]["size"] = size
    return agg


def percentile(name, field, percents=[1, 5, 25, 50, 75, 95, 99]):
    return {
        "name": name,
        "percentile": {
            "field": field, "percents": percents
        }
    }


def histogram(name, field, interval):
    return {
        "name": name,
        "histogram": {
            "field": field, "interval": interval
        }
    }
