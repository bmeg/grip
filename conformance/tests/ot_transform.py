
import gripql

def test_count(man):
    errors = []

    G = man.setGraph("swapi")

    q = G.query().V().hasLabel("Planet").aggregate(gripql.term("t", "terrain"))

    for row in q:
        print(row)

    q = G.query().V().hasLabel("Planet").unwind("terrain").aggregate(gripql.term("t", "terrain"))
    for row in q:
        print(row)
    
    return errors
