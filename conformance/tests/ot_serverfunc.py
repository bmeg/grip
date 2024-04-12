
attr_len_func = """
function attr_len(x) {
    x["skin_colors_len"] = x["skin_colors"].length;
    x["hair_colors_len"] = x["hair_colors"].length;
    x["eye_colors_len"]  = x["eye_colors"].length;
    return [x]
}
"""

def test_flatmap(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    q = G.query().V().hasLabel("Species").flatMap("attr_len", attr_len_func)
    for row in q:
        count += 1
        if row["data"]["skin_colors_len"] != len(row["data"]["skin_colors"]):
            errors.append("count function not correct")
    if count != 5:
        errors.append("Incorrect row count returned: %d != 5" % (count))
    return errors