

def test_list_labels(O, man):
    errors = []

    man.setGraph("swapi")

    resp = O.listLabels()
    print(resp)

    if len(resp["vertex_labels"]) != 6:
        errors.append("listLabels returned an unexpected number of vertex labels; %d != 2" % (len(resp["vertex_labels"])))

    if sorted(resp["vertex_labels"]) != ["Character", "Film", "Planet", "Species", "Starship", "Vehicle"]:
        errors.append("listLabels returned unexpected vertex labels")

    if len(resp["edge_labels"]) != 10:
        errors.append("listLabels returned an unexpected number of edge labels; %d != 2" % (len(resp["edge_labels"])))

    if sorted(resp["edge_labels"]) != ["characters", "films", "homeworld", "people", "pilots", "planets", "residents", "species", "starships", "vehicles"]:
        errors.append("listLabels returned unexpected edge labels")

    return errors
