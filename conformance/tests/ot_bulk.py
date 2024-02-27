import uuid
import datetime
import random


def test_bulkload_scale(man):
    errors = []
    G = man.writeTest()

    bulk = G.bulkAdd()

    for i in range(10000):
        random_time = str(datetime.datetime(2000, 1, 1,
                                            random.choice(range(24)),
                                            random.choice(range(60)),
                                            random.choice(range(60)),
                                            random.choice(range(1000000))))

        observation_template = {"resourceType": "Observation",
                                "id": str(uuid.uuid4()),
                                "meta":
                                    {"versionId": "1",
                                     "lastUpdated": random_time,
                                     "source": "#DmW9sueQ4yuQdyA9",
                                     "profile": ["http://hl7.org/fhir/StructureDefinition/bodyheight",
                                                 "http://hl7.org/fhir/StructureDefinition/vitalsigns"]
                                     },
                                    "status": "final",
                                    "category": [{"coding":
                                                 [{"system": "http://terminology.hl7.org/CodeSystem/observation-category",
                                                  "code": "vital-signs",
                                                   "display": "vital-signs"}]
                                                  }],
                                    "code":
                                        {"coding":
                                            [{"system": "http://loinc.org",
                                             "code": "8302-2", "display": "8302-2"}],
                                         "text": "Body Height"},
                                    "subject": {"reference": f"Patient/{str(uuid.uuid4())}"},
                                    "effectiveDateTime": random_time,
                                    "issued": random_time,
                                    "valueQuantity":
                                        {"value": str(random.choice(range(500))),
                                            "unit": "cm",
                                            "system": "http://unitsofmeasure.org",
                                            "code": "cm"}
                                }
        bulk.addVertex(str(i), str(random.choice(["Patient", "Observation", "File"])), observation_template)
    err = bulk.execute()
    print(err)
    if err.get("errorCount", 0) != 0:
        print(err)
        errors.append("Bulk insertion error")

    res = G.query().V().count().execute()[0]
    if res["count"] != 10000:
        count = 10000
        errors.append(f"Bulk Add wrong number of vertices: {res["count"]} != {count}")

    npatients = G.query().V().hasLabel("Patient").count().execute()[0]["count"]
    nobservations = G.query().V().hasLabel("Observation").count().execute()[0]["count"]
    nfiles = G.query().V().hasLabel("File").count().execute()[0]["count"]

    print(f"npatients: {npatients}, nobservations: {nobservations}, nfiles: {nfiles}")

    if npatients + nobservations + nfiles != 10000:
        errors.append("npatients + nobservations + nfiles != 10000")

    if npatients == 0:
        errors.append(f"npatients == {npatients}")
    if nobservations == 0:
        errors.append(f"nobservations == {nobservations}")
    if nfiles == 0:
        errors.append(f"nfiles == {nfiles}")

    return errors


def test_bulkload(man):
    errors = []

    G = man.writeTest()

    bulk = G.bulkAdd()

    bulk.addVertex("1", "Person", {"name": "marko", "age": "29"})
    bulk.addVertex("2", "Person", {"name": "vadas", "age": "27"})
    bulk.addVertex("3", "Software", {"name": "lop", "lang": "java"})
    bulk.addVertex("4", "Person", {"name": "josh", "age": "32"})
    bulk.addVertex("5", "Software", {"name": "ripple", "lang": "java"})
    bulk.addVertex("6", "Person", {"name": "peter", "age": "35"})

    bulk.addEdge("1", "3", "created", {"weight": 0.4})
    bulk.addEdge("1", "2", "knows", {"weight": 0.5})
    bulk.addEdge("1", "4", "knows", {"weight": 1.0})
    bulk.addEdge("4", "3", "created", {"weight": 0.4})
    bulk.addEdge("6", "3", "created", {"weight": 0.2})
    bulk.addEdge("4", "5", "created", {"weight": 1.0})

    err = bulk.execute()
    print(err)
    if err.get("errorCount", 0) != 0:
        print(err)
        errors.append("Bulk insertion error")

    res = G.query().V().count().execute()[0]
    if res["count"] != 6:
        errors.append(
            "Bulk Add wrong number of vertices: %s != %s" %
            (res["count"], 6))

    res = G.query().E().count().execute()[0]
    if res["count"] != 6:
        errors.append(
            "Bulk Add wrong number of edges: %s != %s" %
            (res["count"], 6))

    return errors


def test_bulkload_validate(man):
    errors = []

    G = man.writeTest()

    bulk = G.bulkAdd()

    bulk.addVertex("1", "Person", {"name": "marko", "age": "29"})
    bulk.addVertex("2", "Person", {"name": "vadas", "age": "27"})
    bulk.addVertex("3", "Software", {"name": "lop", "lang": "java"})
    bulk.addVertex("4", "Person", {"name": "josh", "age": "32"})
    bulk.addVertex("5", "Software", {"name": "ripple", "lang": "java"})
    bulk.addVertex("6", "Person", {"name": "peter", "age": "35"})

    bulk.addEdge("1", None, "created", {"weight": 0.4})
    bulk.addEdge("1", "2", "knows", {"weight": 0.5})
    bulk.addEdge("1", "4", "knows", {"weight": 1.0})
    bulk.addEdge("4", "3", "created", {"weight": 0.4})
    bulk.addEdge("6", "3", "created", {"weight": 0.2})
    bulk.addEdge("4", "5", None, {"weight": 1.0})

    err = bulk.execute()

    if err["errorCount"] == 0:
        errors.append("Validation error not detected")
    print(err)
    return errors
