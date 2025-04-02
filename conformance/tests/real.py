import json

def load_json_schema(path):
    with open(path, 'r') as file:
        content = file.read()
        return json.loads(content)


def test_bulk_add_raw(man):
    errors = []

    G = man.writeTest()
    G.addJsonSchema(load_json_schema("schema.json"))

    with open("Observation.ndjson", "r") as fread:
        count = 0
        bulkRaw = G.bulkAddRaw()  # Start initial stream

        for line in fread:
            bulkRaw.addJson(data=json.loads(line))
            count += 1
            if count % 100000 == 0:
                print(f"read {count} lines")
                err = bulkRaw.execute()  # Execute and close current stream
                print("ERR: ", err)
                if len(err["errors"]) > 0:
                    errors.extend(err["errors"])
                bulkRaw = G.bulkAddRaw()  # Start new stream for next batch

        # Handle remaining items
        if count % 100000 != 0:
            err = bulkRaw.execute()  # Final execution
            print("Final ERR: ", err)
            if len(err["errors"]) > 0:
                errors.extend(err["errors"])

    return errors
