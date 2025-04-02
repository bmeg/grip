

def test_list_labels(man):
    errors = []

    G = man.setGraph("swapi")

    resp = G.listLabels()
    #print(resp)

    if len(resp["vertexLabels"]) != 6:
        errors.append("listLabels returned an unexpected number of vertex labels; %d != 2" % (len(resp["vertex_labels"])))

    if sorted(resp["vertexLabels"]) != ["Character", "Film", "Planet", "Species", "Starship", "Vehicle"]:
        errors.append("listLabels returned unexpected vertex labels")

    if len(resp["edgeLabels"]) != 10:
        errors.append("listLabels returned an unexpected number of edge labels; %d != 10" % (len(resp["edge_labels"])))

    if sorted(resp["edgeLabels"]) != ["characters", "films", "homeworld", "people", "pilots", "planets", "residents", "species", "starships", "vehicles"]:
        errors.append("listLabels returned unexpected edge labels")

    return errors
