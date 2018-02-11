import aql

conn = aql.Connection("http://localhost:8202")
O = conn.graph("example")
print conn.list()
print list(O.query().V())
