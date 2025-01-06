def test_flatmap(man):
    attr_len_func = """
    function attr_len(x, args) {
        x["skin_colors_len"] = x["skin_colors"].length;
        x["hair_colors_len"] = x["hair_colors"].length;
        x["eye_colors_len"]  = x["eye_colors"].length;
        return [x]
    }
    """

    errors = []

    G = man.setGraph("swapi")

    count = 0
    q = G.query().V().hasLabel("Species").flatMap("attr_len", attr_len_func, {})
    for row in q:
        count += 1
        if row["data"]["skin_colors_len"] != len(row["data"]["skin_colors"]):
            errors.append("count function not correct")
    if count != 5:
        errors.append("Incorrect row count returned: %d != 5" % (count))
    return errors


def test_command_line_args(man):
    func = """
    function AddObj(x, args) {
        for (var k in args){
            x[k]= args[k]
        }
        return [x]
    }
    """

    errors = []
    G = man.setGraph("swapi")
    q = G.query().V().hasLabel("Species").flatMap("AddObj", func, {"OtherSpecies":["Sullustan", "RathStar",  "Bith"]})
    for row in q:
        if "OtherSpecies" not in row['data'] or not all(species in row['data']["OtherSpecies"] for species in ["Sullustan", "RathStar", "Bith"]):
            errors.append("Appended items '{'OtherSpecies':['Sullustan', 'RathStar',  'Bith']}' Not Present in row")
    return errors
