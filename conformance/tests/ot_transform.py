import gripql


def test_count(man):
    errors = []

    G = man.setGraph("swapi")

    # Validate equivalent behavior via wildcard list filtering.
    expected_terrains = ["rainforests", "desert", "mountains", "jungle", "grasslands"]
    matched = 0
    for terrain in expected_terrains:
        rows = list(
            G.V().hasLabel("Planet").has(gripql.eq("terrain[*]", terrain)).count()
        )
        print("ROWS: ", rows)
        if len(rows) != 1:
            errors.append("No count returned for terrain[*] == %s" % (terrain))
            continue
        if rows[0]["count"] != 1:
            errors.append(
                "Incorrect count for terrain[*] == %s: %s != %s"
                % (terrain, rows[0]["count"], 1)
            )
        else:
            matched += 1

    if matched != len(expected_terrains):
        errors.append(
            "Incorrect # terrain matches returned %d != %d"
            % (matched, len(expected_terrains))
        )

    return errors


def test_list_wildcard_filtering(man):
    errors = []
    G = man.writeTest()
    bulk = G.bulkAdd()
    bulk.addVertex(
        "1",
        "Observation",
        {
            "resourceType": "Observation",
            "id": "e87cecef-c91d-3861-a35e-6eaed41580c8",
            "status": "final",
            "category": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                            "code": "laboratory",
                            "display": "laboratory",
                        }
                    ]
                }
            ],
            "code": {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": "81247-9",
                        "display": "Master HL7 genetic variant reporting panel",
                    }
                ]
            },
            "subject": {"reference": "Patient/ac0d7a82-82cb-4aec-b859-e37375f3de8b"},
            "specimen": {"reference": "Specimen/dc48f578-193c-4740-93f3-61a78e3c6ba0"},
            "focus": [{"reference": "Specimen/dc48f578-193c-4740-93f3-61a78e3c6ba0"}],
            "effectiveDateTime": "2024-06-03T08:00:00+00:00",
            "valueString": "Sequencing parameters",
            "component": [
                {
                    "code": {
                        "coding": [
                            {
                                "system": "https://cadsr.cancer.gov/sample_laboratory_observation",
                                "code": "concentration",
                                "display": "concentration",
                            }
                        ],
                        "text": "concentration",
                    },
                    "valueQuantity": {"value": 0.16},
                },
                {
                    "code": {
                        "coding": [
                            {
                                "system": "https://cadsr.cancer.gov/sample_laboratory_observation",
                                "code": "aliquot_quantity",
                                "display": "aliquot_quantity",
                            }
                        ],
                        "text": "aliquot_quantity",
                    },
                    "valueQuantity": {"value": 2.13},
                },
                {
                    "code": {
                        "coding": [
                            {
                                "system": "https://cadsr.cancer.gov/sample_laboratory_observation",
                                "code": "aliquot_volume",
                                "display": "aliquot_volume",
                            }
                        ],
                        "text": "aliquot_volume",
                    },
                    "valueQuantity": {"value": 13.3},
                },
            ],
        },
    )
    err = bulk.execute()

    # Validate list behavior via wildcard filtering/render.
    q = G.V().hasLabel("Observation").render(["component[*].valueQuantity.value"])
    rows = list(q)
    if len(rows) != 1:
        errors.append("There should be 1 Observation row")
    elif not isinstance(rows[0][0], list):
        errors.append("component[*].valueQuantity.value should return a list")
    elif len(rows[0][0]) != 3:
        errors.append("component[*].valueQuantity.value should contain 3 values")

    # Any-match semantics on wildcard list paths.
    gt_rows = list(
        G.V()
        .hasLabel("Observation")
        .has(gripql.gt("component[*].valueQuantity.value", 1))
    )
    if len(gt_rows) != 1:
        errors.append("There should be 1 vertex after wildcard list filter")

    eq_rows = list(
        G.V()
        .hasLabel("Observation")
        .has(gripql.eq("component[*].valueQuantity.value", 2.13))
    )
    if len(eq_rows) != 1:
        errors.append("There should be 1 vertex for eq on wildcard list path")

    return errors


def test_group_totype_list_paths(man):
    errors = []
    G = man.writeTest()
    bulk = G.bulkAdd()
    bulk.addVertex(
        "1",
        "Observation",
        {
            "resourceType": "Observation",
            "id": "e87cecef-c91d-3861-a35e-6eaed41580c8",
            "status": "final",
            "category": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                            "code": "laboratory",
                            "display": "laboratory",
                        }
                    ]
                }
            ],
            "code": {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": "81247-9",
                        "display": "Master HL7 genetic variant reporting panel",
                    }
                ]
            },
            "subject": {"reference": "Patient/ac0d7a82-82cb-4aec-b859-e37375f3de8b"},
            "specimen": {"reference": "Specimen/dc48f578-193c-4740-93f3-61a78e3c6ba0"},
            "focus": [{"reference": "Specimen/dc48f578-193c-4740-93f3-61a78e3c6ba0"}],
            "effectiveDateTime": "2024-06-03T08:00:00+00:00",
            "valueString": "Sequencing parameters",
            "component": [
                {
                    "code": {
                        "coding": [
                            {
                                "system": "https://cadsr.cancer.gov/sample_laboratory_observation",
                                "code": "concentration",
                                "display": "concentration",
                            }
                        ],
                        "text": "concentration",
                    },
                    "valueQuantity": {"value": 0.16},
                },
                {
                    "code": {
                        "coding": [
                            {
                                "system": "https://cadsr.cancer.gov/sample_laboratory_observation",
                                "code": "aliquot_quantity",
                                "display": "aliquot_quantity",
                            }
                        ],
                        "text": "aliquot_quantity",
                    },
                    "valueQuantity": {"value": 2.13},
                },
                {
                    "code": {
                        "coding": [
                            {
                                "system": "https://cadsr.cancer.gov/sample_laboratory_observation",
                                "code": "aliquot_volume",
                                "display": "aliquot_volume",
                            }
                        ],
                        "text": "aliquot_volume",
                    },
                    "valueQuantity": {"value": 13.3},
                },
            ],
        },
    )

    bulk.addVertex(
        "2",
        "Specimen",
        {
            "resourceType": "Specimen",
            "id": "dc48f578-193c-4740-93f3-61a78e3c6ba0",
            "status": "final",
            "category": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                            "code": "laboratory",
                            "display": "laboratory",
                        }
                    ]
                }
            ],
            "code": {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": "81247-9",
                        "display": "Master HL7 genetic variant reporting panel",
                    }
                ]
            },
            "subject": {"reference": "Patient/ac0d7a82-82cb-4aec-b859-e37375f3de8b"},
            "specimen": {"reference": "Specimen/dc48f578-193c-4740-93f3-61a78e3c6ba0"},
            "focus": [{"reference": "Specimen/dc48f578-193c-4740-93f3-61a78e3c6ba0"}],
            "effectiveDateTime": "2024-06-03T08:00:00+00:00",
            "valueString": "Sequencing parameters",
            "component": [
                {
                    "code": {
                        "coding": [
                            {
                                "system": "https://cadsr.cancer.gov/sample_laboratory_observation1",
                                "code": "concentration",
                                "display": "concentration",
                            }
                        ],
                        "text": "concentration",
                    },
                    "valueQuantity": {"value": 0.16},
                },
                {
                    "code": {
                        "coding": [
                            {
                                "system": "https://cadsr.cancer.gov/sample_laboratory_observation2",
                                "code": "aliquot_quantity",
                                "display": "aliquot_quantity",
                            }
                        ],
                        "text": "aliquot_quantity",
                    },
                    "valueQuantity": {"value": 2.13},
                },
                {
                    "code": {
                        "coding": [
                            {
                                "system": "https://cadsr.cancer.gov/sample_laboratory_observation3",
                                "code": "aliquot_volume",
                                "display": "aliquot_volume",
                            }
                        ],
                        "text": "aliquot_volume",
                    },
                    "valueQuantity": {"value": 13.3},
                },
            ],
        },
    )
    bulk.addEdge("1", "2", "focus_Specimen")
    err = bulk.execute()

    orig_row = {}
    for i in G.V().hasLabel("Observation").as_("f0").out("focus_Specimen"):
        orig_row = i

    # Validate type/group behavior by operating on indexed list paths.
    q = (
        G.V()
        .hasLabel("Observation")
        .as_("f0")
        .out("focus_Specimen")
        .as_("f1")
        .totype("$f1.component[0].code.coding", "list")
        .group({"component0": "$f1.component[0]"})
    )
    for i in q:
        if not isinstance(i["component0"], list) or len(i["component0"]) == 0:
            errors.append("group output missing component0 list")
            continue
        if not isinstance(i["component0"][0]["code"]["coding"], list) and isinstance(
            orig_row["component"][0]["code"]["coding"], list
        ):
            errors.append(
                "Original row list format not preserved: %s !=\n\n %s" % (orig_row, i)
            )

    return errors
