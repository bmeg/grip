

def test_select_from_bundle(O):
    errors = []

    O.addVertex("srcVertex", "person")
    for i in range(100):
        O.addVertex("dstVertex%d" % i, "person")

    edges = {}
    for i in range(100):
        edges["dstVertex%d" % i] = {"val" : i}
    O.addBundle("srcVertex", edges, "bundle1")

    edges = {}
    for i in range(100):
        edges["dstVertex%d" % i] = {"val" : i}
    O.addBundle("srcVertex", edges, "bundle2")

    count = 0
    for i in O.query().V("srcVertex").match([
        O.mark("a").outgoingBundle("bundle1").mark("b"),
        O.mark("a").outgoingBundle("bundle2").mark("c")
    ]).vertexFromValues("""
function(x){
    return _.filter(_.intersection(_.keys(x.b.bundle),_.keys(x.c.bundle)), function(i){
        return (x.b.bundle[i].val % 2 == 0) && (x.c.bundle[i].val % 3 == 0)
    } )
} """).mark("d").select(["d"]).execute():
        count += 1
        val = int(i[0]['vertex']['gid'].replace("dstVertex", ""))
        if val % 2 != 0 or val % 3 != 0:
            errors.append("Fail: Incorrect vertex: %s" % (i[0]['vertex']['gid']))

    if count != 17:
        errors.append("Fail: Bundle Filter %s != %s" % (count, 17))

    return errors
