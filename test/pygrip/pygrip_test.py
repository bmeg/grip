import pygrip

w = pygrip.NewMemServer()


w.addVertex("1", "Person", {"age":30, "eyes":"brown"})
w.addVertex("2", "Person", {"age":40, "eyes":"blue"})

w.addEdge("1", "2", "knows")

for row in w.V().hasLabel("Person"):
    print("hasLabel", row)

for row in w.V().out("knows"):
    print("out", row)

for row in w.V().count():
    print("count", row)

