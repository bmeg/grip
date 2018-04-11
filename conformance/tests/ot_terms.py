

def test_simple_terms(O):
    """
    test expected terms aggregation.
    this is "simple" since it does not consider the traversal context
    inspired by
    https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html
    """
    errors = []

    O.addVertexIndex("Person", "name")

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

    # aggregate all the terms in 'name'
    aggregations = O.index().aggregate(
        {
            "names": {
                "terms": {"label": "Person", "field": "name", "size": 10}
            }
        }
    )
    if "names" not in aggregations:
        errors.append("'names' should be in the response")
    aggregation = aggregations['names']
    if len(aggregation['rows']) != 6:
        errors.append("There should be 6 people returned from terms")
    if aggregation['rows'][0]['count'] != 2:
        errors.append("marko should have a count of two")
    if aggregation['rows'][0]['term'] != 'marko':
        errors.append("marko should be the 1st term")
    if aggregation['sum_other_doc_count'] != 0:
        errors.append("all terms should be returned since less than size")

    # aggregate 1st the term in 'name'
    aggregations = O.index().aggregate(
        {
            "names": {
                "terms": {"label": "Person", "field": "name", "size": 1}
            }
        }
    )
    if "names" not in aggregations:
        errors.append("'names' should be in the response")
    aggregation = aggregations['names']
    if len(aggregation['rows']) != 1:
        errors.append("There should be 1 person returned from terms")
    if aggregation['rows'][0]['term'] != 'marko':
        errors.append("marko should be the only term")
    if aggregation['sum_other_doc_count'] != 5:
        errors.append("other terms should be returned since more than size")

    try:
        # only terms
        aggregations = O.index().aggregate(
            {
                "names": {
                    "XXX": {"label": "Person", "field": "name", "size": 1}
                }
            }
        )
        errors.append("only terms aggregation supported")
    except Exception as e:
        pass

    try:
        # only existing labels
        aggregations = O.index().aggregate(
            {
                "names": {
                    "terms": {"label": "XXX", "field": "name", "size": 1}
                }
            }
        )
        errors.append(" only existing labels supported")
    except Exception as e:
        pass

    try:
        # only existing fields
        aggregations = O.index().aggregate(
            {
                "names": {
                    "terms": {"label": "Person", "field": "XXX", "size": 1}
                }
            }
        )
        errors.append(" only existing fields supported")
    except Exception as e:
        pass

    return errors
