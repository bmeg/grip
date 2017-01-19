
import ophion

O = ophion.Ophion('http://localhost:8000')

print O.query().addV("test").execute()

print O.query().V().execute()

print O.query().V().count().execute()
