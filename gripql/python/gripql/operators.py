from __future__ import absolute_import, print_function, unicode_literals


def and_(*expressions):
    return {"and": {"expressions": expressions}}


def or_(*expressions):
    return {"or": {"expressions": expressions}}


def not_(expression):
    return {"not": expression}


def eq(key, value):
    return {"condition": {"key": key, "value": value, "condition": "EQ"}}


def neq(key, value):
    return {"condition": {"key": key, "value": value, "condition": "NEQ"}}


def gt(key, value):
    return {"condition": {"key": key, "value": value, "condition": "GT"}}


def gte(key, value):
    return {"condition": {"key": key, "value": value, "condition": "GTE"}}


def lt(key, value):
    return {"condition": {"key": key, "value": value, "condition": "LT"}}


def lte(key, value):
    return {"condition": {"key": key, "value": value, "condition": "LTE"}}


def in_(key, values):
    if not isinstance(values, list):
        if not isinstance(values, dict):
            values = [values]
    return {"condition": {"key": key, "value": values, "condition": "IN"}}


def contains(key, value):
    return {"condition": {"key": key, "value": value, "condition": "CONTAINS"}}
