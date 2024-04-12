
attr_len_func = """
func attr_len(x) {
    x['skin_colors_len'] = len(x['skin_colors'])
    x['hair_colors_len'] = len(x['hair_colors'])
    x['eye_colors_len']  = len(x['eye_colors'])
    return [x]
}
"""

def test_flatmap(man):
    errors = []

    G = man.setGraph("swapi")

    q = G.query().V().hasLabel("Species").flatMap("attr_len", attr_len_func)
    for row in q:
        print(row)

    return errors