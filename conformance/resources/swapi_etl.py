import json
import os
import swapi


edge_lookup = {
    "films": "Film",
    "homeworld": "Planet",
    "species": "Species",
    "starships": "Starship",
    "vehicles": "Vehicle",
    "residents": "Character",
    "characters": "Character",
    "planets": "Planet",
    "species": "Species",
    "people": "Character",
    "pilots": "Character",
}

int_vars = [
    "height", "mass",  # character
    "orbital_period", "population", "rotation_period", "surface_water"  # planet
    "average_height", "average_lifespan",  # species
    "cargo_capacity", "passengers", "crew", "cost_in_credits", "max_atmosphering_speed"  # vehicle /starship
]

float_vars = [
    "gravity",  # planet
    "hyperdrive_rating", "length"  # starship / vehicle
]

str_list_vars = [
    "producer",  # films
    "manufacturer"  # starship / vehicle
    "climate", "terrain",  # planet
    "eye_colors", "hair_colors", "skin_colors"  # species
]

sys_vars = [
    "created", "edited"
]


def create_vertex(label, data):
    gid = data["url"].replace("https://swapi.co/api/", "").strip("/").replace("/", ":")
    tdata = {"system": {}}
    for k, v in data.items():
        if v == "n/a":
            v = None
        if k in int_vars:
            try:
                tdata[k] = int(v)
            except Exception:
                tdata[k] = None
        elif k in float_vars:
            try:
                tdata[k] = float(v)
            except Exception:
                tdata[k] = None
        elif k in str_list_vars:
            try:
                tdata[k] = [x.strip() for x in v.split(",")]
            except Exception:
                tdata[k] = []
        elif k in sys_vars:
            tdata["system"][k] = v
        elif k in edge_lookup:
            continue
        else:
            tdata[k] = v
    return {"gid": gid, "label": label, "data": tdata}


def create_edge(label, fid, tid):
    fid = fid.replace("https://swapi.co/api/", "").strip("/").replace("/", ":")
    tid = tid.replace("https://swapi.co/api/", "").strip("/").replace("/", ":")
    return {"gid": "(%s)-[%s]->(%s)" % (fid, label, tid),
            "label": label, "from": fid, "to": tid}


def create_all_edges(doc):
    edges = []
    for k, v in doc.items():
        if k in edge_lookup:
            if isinstance(v, list):
                for tid in v:
                    edges.append(create_edge(k, doc["url"], tid))
            elif isinstance(v, str):
                edges.append(create_edge(k, doc["url"], v))
            elif v is None:
                continue
            else:
                raise TypeError("unexpected type encountered for edge key %s: %s" % (k, type(v)))
    return edges


films = swapi.get_all('films').items
people = swapi.get_all('people').items
planets = swapi.get_all('planets').items
species = swapi.get_all('species').items
starships = swapi.get_all('starships').items
vehicles = swapi.get_all('vehicles').items

nmap = {"Film": films, "Character": people, "Planet": planets,
        "Species": species, "Starship": starships, "Vehicle": vehicles}

vert_fh = open("swapi_vertices.json", "w")
edge_fh = open("swapi_edges.json", "w")

for label, nodes in nmap.items():
    for node in nodes:
        node = node.__dict__
        v = create_vertex(label, node)
        vert_fh.write(json.dumps(v))
        vert_fh.write(os.linesep)
        for e in create_all_edges(node):
            edge_fh.write(json.dumps(e))
            edge_fh.write(os.linesep)

vert_fh.close()
edge_fh.close()
