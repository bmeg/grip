from __future__ import absolute_import, print_function, unicode_literals


def term(name, label, field, size=None):
    agg = {
        "name": name,
        "term": {"label": label, "field": field}
    }
    if size:
        agg["term"]["size"] = size
    return agg


def percentile(name, label, field, percents=[1, 5, 25, 50, 75, 95, 99]):
    return {
        "name": name,
        "percentile": {
            "label": label, "field": field, "percents": percents
        }
    }


def histogram(name, label, field, interval):
    return {
        "name": name,
        "histogram": {
            "label": label, "field": field, "interval": interval
        }
    }
