
import ophion

G = ophion.Ophion('http://localhost:8000')



print G.query().addV("vertex1").property("field1", "value1").property("field2", "value2").execute()
print G.query().addV("vertex2").execute()
print G.query().addV("vertex3").property("field1", "value3").property("field2", "value4").execute()
print G.query().addV("vertex4").execute()

print G.query().V("vertex1").addE("friend").to("vertex2").execute()
print G.query().V("vertex2").addE("friend").to("vertex3").execute()
print G.query().V("vertex2").addE("parent").to("vertex4").execute()

for i in G.query().V().execute():
    print "found vertex", i

for i in G.query().E().execute():
    print "found edge", i

for i in G.query().V("vertex1").outgoing().execute():
    print "found vertex", i

for i in G.query().V("vertex1").outgoing().outgoing().has("field1", "value4").incoming().execute():
    print "found vertex", i
