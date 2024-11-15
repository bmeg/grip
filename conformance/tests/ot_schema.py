
import requests

def test_getscheama(man):
    errors = []

    G = man.setGraph("swapi")

    s = G.sampleSchema()

    vLabels = sorted( list(v['gid'] for v in s['vertices']) )

    vExpectedLabels = [
        'Character', 'Film', 'Planet', 'Species', 'Starship', 'Vehicle'
    ]

    if vLabels != vExpectedLabels:
        errors.append(
            "Incorrect labels returned from sampling %s != %s" %
                (vLabels, vExpectedLabels)
        )

    eExpectedLabels = ["characters", "films", "homeworld", "people",
        "pilots", "planets", "residents", "species", "starships", "vehicles"]
    eLabels = sorted( list( set( v['label'] for v in s['edges']) ) )
    if eLabels != eExpectedLabels:
        errors.append("Incorrect labels returned from sampling: %s != %s " %
            (eLabels, eExpectedLabels)
        )
    return errors


def test_post_json_schema(man):
    errors = []
    G = man.setGraph("swapi")
    # Probably don't want to add a 2MB schema file to the repo so get it via requests instead
    res = requests.get("https://raw.githubusercontent.com/bmeg/iceberg/f1724941fe47df24846135fb515d1b89e791cee3/schemas/graph/graph-fhir.json")
    res.raise_for_status()
    s = G.addJsonSchema(res.json())
    fetched_schema = G.getSchema()
    len_vertices = len(fetched_schema['vertices'])
    if len_vertices != 137:
        errors.append(f"incorrect number of vertices in schema {len_vertices} != 137")
    len_edges = len(fetched_schema['edges'])
    if len_edges != 0:
        errors.append(f"incorrect number of edges in schema {len_vertices} != 0")

    return errors
