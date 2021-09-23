

def test_getscheama(man):
    errors = []

    G = man.setGraph("swapi")

    s = G.sampleSchema()

    vLabels = sorted( list(v['gid'] for v in s['vertices']) )

    if vLabels != ['Character', 'Film', 'Planet', 'Species', 'Starship', 'Vehicle']:
        errors.append("Incorrect labels returned from sampling")

    return errors
