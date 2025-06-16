import gripql

def test_cond_nested_field(man):
    # Tests nested filters on optimized grids query driver

    errors = []
    G = man.writeTest()

    G.addIndex("Observation", "code.coding.[0].code")

    G.addVertex("1", "Observation",  {"resourceType": "Observation", "id": "e87cecef-c91d-3861-a35e-6eaed41580c8", "status": "final", "category": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/observation-category", "code": "laboratory", "display": "laboratory"}]}], "code": {"coding": [{"system": "http://loinc.org", "code": "81247-9", "display": "Master HL7 genetic variant reporting panel"}]}, "subject": {"reference": "Patient/ac0d7a82-82cb-4aec-b859-e37375f3de8b"}, "specimen": {"reference": "Specimen/dc48f578-193c-4740-93f3-61a78e3c6ba0"}, "focus": [{"reference": "Specimen/dc48f578-193c-4740-93f3-61a78e3c6ba0"}], "effectiveDateTime": "2024-06-03T08:00:00+00:00", "valueString": "Sequencing parameters", "component": [{"code": {"coding": [{"system": "https://cadsr.cancer.gov/sample_laboratory_observation", "code": "concentration", "display": "concentration"}], "text": "concentration"}, "valueQuantity": {"value": 0.16}}, {"code": {"coding": [{"system": "https://cadsr.cancer.gov/sample_laboratory_observation", "code": "aliquot_quantity", "display": "aliquot_quantity"}], "text": "aliquot_quantity"}, "valueQuantity": {"value": 2.13}}, {"code": {"coding": [{"system": "https://cadsr.cancer.gov/sample_laboratory_observation", "code": "aliquot_volume", "display": "aliquot_volume"}], "text": "aliquot_volume"}, "valueQuantity": {"value": 13.3}}]})
    G.addVertex("2", "Observation",  {"resourceType": "Observation", "id": "f87cecef-c91d-3861-a35e-6eaed41580c8", "status": "final", "category": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/observation-category", "code": "laboratory", "display": "laboratory"}]}], "code": {"coding": [{"system": "http://loinc.org", "code": "81247-8", "display": "Master HL7 genetic variant reporting panel"}]}, "subject": {"reference": "Patient/ac0d7a82-82cb-4aec-b859-e37375f3de8b"}, "specimen": {"reference": "Specimen/dc48f578-193c-4740-93f3-61a78e3c6ba0"}, "focus": [{"reference": "Specimen/dc48f578-193c-4740-93f3-61a78e3c6ba0"}], "effectiveDateTime": "2024-06-03T08:00:00+00:00", "valueString": "Sequencing parameters", "component": [{"code": {"coding": [{"system": "https://cadsr.cancer.gov/sample_laboratory_observation", "code": "concentration", "display": "concentration"}], "text": "concentration"}, "valueQuantity": {"value": 0.16}}, {"code": {"coding": [{"system": "https://cadsr.cancer.gov/sample_laboratory_observation", "code": "aliquot_quantity", "display": "aliquot_quantity"}], "text": "aliquot_quantity"}, "valueQuantity": {"value": 2.13}}, {"code": {"coding": [{"system": "https://cadsr.cancer.gov/sample_laboratory_observation", "code": "aliquot_volume", "display": "aliquot_volume"}], "text": "aliquot_volume"}, "valueQuantity": {"value": 13.3}}]})

    count = 0
    for i in G.query().V().has(gripql.eq("code.coding.[0].code","81247-8")):
        count +=1

    if count != 1:
        errors.append("Expected count == 1 but got %d instead" % (count))

    return errors


def test_bulk_cond_nested_field(man):
    errors = []
    G = man.writeTest()
    G.addIndex("Observation", "code.coding.[0].code")

    bulk = G.bulkAdd()
    bulk.addVertex("1", "Observation",  {"resourceType": "Observation", "id": "e87cecef-c91d-3861-a35e-6eaed41580c8", "status": "final", "category": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/observation-category", "code": "laboratory", "display": "laboratory"}]}], "code": {"coding": [{"system": "http://loinc.org", "code": "81247-9", "display": "Master HL7 genetic variant reporting panel"}]}, "subject": {"reference": "Patient/ac0d7a82-82cb-4aec-b859-e37375f3de8b"}, "specimen": {"reference": "Specimen/dc48f578-193c-4740-93f3-61a78e3c6ba0"}, "focus": [{"reference": "Specimen/dc48f578-193c-4740-93f3-61a78e3c6ba0"}], "effectiveDateTime": "2024-06-03T08:00:00+00:00", "valueString": "Sequencing parameters", "component": [{"code": {"coding": [{"system": "https://cadsr.cancer.gov/sample_laboratory_observation", "code": "concentration", "display": "concentration"}], "text": "concentration"}, "valueQuantity": {"value": 0.16}}, {"code": {"coding": [{"system": "https://cadsr.cancer.gov/sample_laboratory_observation", "code": "aliquot_quantity", "display": "aliquot_quantity"}], "text": "aliquot_quantity"}, "valueQuantity": {"value": 2.13}}, {"code": {"coding": [{"system": "https://cadsr.cancer.gov/sample_laboratory_observation", "code": "aliquot_volume", "display": "aliquot_volume"}], "text": "aliquot_volume"}, "valueQuantity": {"value": 13.3}}]})
    bulk.addVertex("2", "Observation",  {"resourceType": "Observation", "id": "f87cecef-c91d-3861-a35e-6eaed41580c8", "status": "final", "category": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/observation-category", "code": "laboratory", "display": "laboratory"}]}], "code": {"coding": [{"system": "http://loinc.org", "code": "81247-8", "display": "Master HL7 genetic variant reporting panel"}]}, "subject": {"reference": "Patient/ac0d7a82-82cb-4aec-b859-e37375f3de8b"}, "specimen": {"reference": "Specimen/dc48f578-193c-4740-93f3-61a78e3c6ba0"}, "focus": [{"reference": "Specimen/dc48f578-193c-4740-93f3-61a78e3c6ba0"}], "effectiveDateTime": "2024-06-03T08:00:00+00:00", "valueString": "Sequencing parameters", "component": [{"code": {"coding": [{"system": "https://cadsr.cancer.gov/sample_laboratory_observation", "code": "concentration", "display": "concentration"}], "text": "concentration"}, "valueQuantity": {"value": 0.16}}, {"code": {"coding": [{"system": "https://cadsr.cancer.gov/sample_laboratory_observation", "code": "aliquot_quantity", "display": "aliquot_quantity"}], "text": "aliquot_quantity"}, "valueQuantity": {"value": 2.13}}, {"code": {"coding": [{"system": "https://cadsr.cancer.gov/sample_laboratory_observation", "code": "aliquot_volume", "display": "aliquot_volume"}], "text": "aliquot_volume"}, "valueQuantity": {"value": 13.3}}]})

    bulk.execute()

    count = 0
    for i in G.query().V().has(gripql.eq("code.coding.[0].code","81247-8")):
        count +=1

    if count != 1:
        errors.append("Expected count == 1 but got %d instead" % (count))

    return errors
