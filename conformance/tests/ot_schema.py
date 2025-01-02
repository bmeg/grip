
import json
from pylint import graph

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
    G.addJsonSchema(load_json_schema("conformance/graphs/prompt-schema.json"))
    fetched_schema = G.getSchema()
    len_vertices = len(fetched_schema['vertices'])
    if len_vertices != 2:
        errors.append(f"incorrect number of vertices in schema {len_vertices} != 2")
    len_edges = len(fetched_schema['edges'])
    if len_edges != 0:
        errors.append(f"incorrect number of edges in schema {len_vertices} != 0")

    return errors


def load_json_schema(path):
    with open(path, 'r') as file:
        content = file.read()
        return json.loads(content)
