
import sys
import json
import ophion

o = ophion.Ophion(sys.argv[1])

print o.query().V().count().execute()
print o.query().E().count().execute()

#print o.query().V().has("group", "Book").count().execute()

#print json.dumps(o.query().V().limit(10).execute(), indent=4)

#print json.dumps(o.query().V().has("Id", "1").outgoing("similar").execute(), indent=4)

#print json.dumps(o.query().V().has("group", "Book").as("a").outgoing("similar").has("group", "DVD").count().execute(), indent=4)
