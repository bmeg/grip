
import gripql

def test_count(man):
    errors = []

    G = man.setGraph("swapi")

    q = G.query().V().hasLabel("Planet").unwind("terrain").aggregate(gripql.term("t", "terrain"))
    count = 0
    for row in q:
        if row['key'] not in ['rainforests', 'desert', 'mountains', 'jungle', 'rainforests', 'grasslands']:
            errors.append("Incorrect value %s returned" % row['key'])
        if row['value'] != 1:
            errors.append("Incorrect count returned")
        count += 1

    if count != 5:
        errors.append("Incorrect # elements returned")

    return errors
