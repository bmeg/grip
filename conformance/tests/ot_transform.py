
from re import I
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


def test_unwind(man):
    errors = []
    G = man.writeTest()
    bulk = G.bulkAdd()
    bulk.addVertex("1", "Observation", {"resourceType": "Observation", "id": "e87cecef-c91d-3861-a35e-6eaed41580c8", "status": "final", "category": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/observation-category", "code": "laboratory", "display": "laboratory"}]}], "code": {"coding": [{"system": "http://loinc.org", "code": "81247-9", "display": "Master HL7 genetic variant reporting panel"}]}, "subject": {"reference": "Patient/ac0d7a82-82cb-4aec-b859-e37375f3de8b"}, "specimen": {"reference": "Specimen/dc48f578-193c-4740-93f3-61a78e3c6ba0"}, "focus": [{"reference": "Specimen/dc48f578-193c-4740-93f3-61a78e3c6ba0"}], "effectiveDateTime": "2024-06-03T08:00:00+00:00", "valueString": "Sequencing parameters", "component": [{"code": {"coding": [{"system": "https://cadsr.cancer.gov/sample_laboratory_observation", "code": "concentration", "display": "concentration"}], "text": "concentration"}, "valueQuantity": {"value": 0.16}}, {"code": {"coding": [{"system": "https://cadsr.cancer.gov/sample_laboratory_observation", "code": "aliquot_quantity", "display": "aliquot_quantity"}], "text": "aliquot_quantity"}, "valueQuantity": {"value": 2.13}}, {"code": {"coding": [{"system": "https://cadsr.cancer.gov/sample_laboratory_observation", "code": "aliquot_volume", "display": "aliquot_volume"}], "text": "aliquot_volume"}, "valueQuantity": {"value": 13.3}}]})
    err = bulk.execute()

    q = G.query().V().hasLabel("Observation").unwind("component")
    count = 0
    for row in q:
        #print("ROW: ", row)
        count +=1
    if count != 3:
        errors.append("There should be 3 vertices after unwind process")

    q = G.query().V().hasLabel("Observation").unwind("component").has(gripql.gt("component.valueQuantity.value", 1))
    count = 0
    for r in q:
        count +=1
    if count != 2:
        errors.append("There should be 2 vertices after unwind process and filter")

    return errors
