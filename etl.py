import json


def extract_ids_from_ndjson(input_file, output_file):
    ids = []

    # Read the NDJSON file
    with open(input_file, 'r') as f:
        for line in f:
            data = json.loads(line.strip())
            ids.append(data['gid'])

    with open(input_file_edge, 'r') as f:
        for line in f:
            data = json.loads(line.strip())
            ids.append(data['gid'])


    # Write the IDs to the output file in the specified format
    with open(output_file, 'w') as f:
        f.write('[' + ','.join([f'"{gid}"' for gid in ids]) + ']')

# Specify the input and output file paths
input_file = 'OUT/Observation.vertex.json'
input_file_edge= 'OUT/Observation.in.edge.json'
output_file = 'output.json'

# Extract the IDs and write them to the output file
extract_ids_from_ndjson(input_file, output_file)
