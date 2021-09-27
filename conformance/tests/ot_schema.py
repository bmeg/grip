

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
