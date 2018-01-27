


def test_fold(O):
    errors = []

    graph = {
        "vertices" : [
            {"gid": "1", "label": "Person", "data" : {"name":"marko", "age":"29"}},
            {"gid": "2", "label": "Person", "data" : {"name":"vadas", "age":"27"}},
            {"gid": "3", "label": "Software", "data" : {"name":"lop", "lang":"java"}},
            {"gid": "4", "label": "Person", "data" : {"name":"josh", "age":"32"}},
            {"gid": "5", "label": "Software", "data" : {"name":"ripple", "lang":"java"}},
            {"gid": "6", "label": "Person", "data" : {"name":"peter", "age":"35"}},
        ],
        "edges" : [
            {"from": "1", "to": "3", "label": "created", "data" :{"weight":0.4}},
            {"from": "1", "to": "2", "label": "knows","data" : {"weight":0.5}},
            {"from": "1", "to": "4", "label": "knows", "data" :{"weight":1.0}},
            {"from": "4", "to": "3", "label": "created", "data" :{"weight":0.4}},
            {"from": "6", "to": "3", "label": "created","data" : {"weight":0.2}},
            {"from": "4", "to": "5", "label": "created", "data" :{"weight":1.0}}
        ]
    }

    O.addSubGraph(graph)

    foldFunc = """function(x,y){var z={}; z[y["name"]] = true; return _.extend(x,z) }"""
    query = O.query().V().fold({}, foldFunc)

    for row in query:
        print row

    return errors
