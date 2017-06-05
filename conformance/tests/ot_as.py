



def test_as_select(O):
    errors = []

    O.query().addV("vertex1").property("field1", "value1").property("field2", "value2").execute()
    O.query().addV("vertex2").execute()
    O.query().addV("vertex3").property("field1", "value3").property("field2", "value4").execute()
    O.query().addV("vertex4").execute()

    O.query().V("vertex1").addE("friend").to("vertex2").execute()
    O.query().V("vertex2").addE("friend").to("vertex3").execute()
    O.query().V("vertex2").addE("parent").to("vertex4").execute()

    for row in O.query().V("vertex1").mark("a").outgoing().mark("b").outgoing().mark("c").select(["a", "b", "c"]).execute():
        res = dict(zip(["a","b","c"], row))
        if res["a"]["vertex"]["gid"] != "vertex1":
            errors.append("Incorrect as selection")
        if res["b"]["vertex"]["gid"] != "vertex2":
            errors.append("Incorrect as selection")
        if res["c"]["vertex"]["gid"] not in ["vertex3", "vertex4"]:
            errors.append("Incorrect as selection")

    return errors
