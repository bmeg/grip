from __future__ import absolute_import, print_function, unicode_literals

import json
import os
import sys
import string
import random
import gripql
import re
from kafka import KafkaConsumer
import logging
import time
import requests

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define BASE directory
BASE = os.path.dirname(os.path.abspath(__file__))
TOPIC = "gripHistory"
KAFKA_HOST = "localhost:9092"
GRIP_SERVER_URL = "http://localhost:8201"
USERNAME = "admin"
PASSWORD = "adminpassword"
TIMEOUTMS = "1000"


class SkipTest(Exception):
    """A target test can raise this to ignore test."""
    pass

class Manager:
    """Common test methods."""
    def __init__(self, conn, readOnly=False, server=None, grip_config_file_path=None):
        self.readOnly = readOnly
        self.curGraph = ""
        self.curName = ""
        self.grip_config = None
        self.access_casbin = None
        self.policies = None
        self.accounts = []
        self.all_graph_names = []
        self.graphs = None
        self._conn = None
        self.user = None
        self.server = server
        self.set_connection(conn)
        self.kafka_consumer = self.init_kafka_consumer()
        self.loadgraphname = "swapi"

    def set_connection(self, conn):
        """Set conn and user property"""
        self._conn = conn
        if self._conn:
            self.user = self._conn.user
        else:
            self.user = None

    def collect_fields_dict(self, datadict):
        """Filter out reserved fields."""
        if not isinstance(datadict, dict):
            logger.error(f"Expected dict, got {type(datadict)}: {datadict}")
            return {}
        return {key: value for key, value in datadict.items() if key not in ["_id", "_label", "_from", "_to"]}

    def clean(self, edges, vertices):
        """Delete current graph if not in readOnly mode."""
        if edges is not None and len(edges) > 0 and vertices is not None and len(vertices) > 0:
            self._conn.graph(self.curGraph).delete(edges=edges, vertices=vertices)
            self._conn.deleteGraph(self.curGraph)


    def id_generator(self, size=6, chars=string.ascii_uppercase + string.digits):
        """Random 6 alphanumeric string."""
        return ''.join(random.choice(chars) for _ in range(size)).lower()

    def init_kafka_consumer(self):
        """Initialize Kafka consumer for TOPIC."""
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[KAFKA_HOST],
                security_protocol='SASL_PLAINTEXT',
                sasl_mechanism='PLAIN',
                sasl_plain_username=USERNAME,
                sasl_plain_password=PASSWORD,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: self.deserialize_message(x),
                session_timeout_ms=TIMEOUTMS
            )
            logger.info("Connected to Kafka consumer")
            return consumer
        except Exception as e:
            logger.error(f"Failed to connect to Kafka consumer: {e}")
            return None

    def deserialize_message(self, data):
        """Safely deserialize Kafka message."""
        if not data:
            logger.debug("Received empty Kafka message")
            return ""
        try:
            decoded = data.decode('utf-8')
            return json.loads(decoded)
        except json.JSONDecodeError:
            return decoded
        except UnicodeDecodeError:
            logger.warning(f"Failed to decode message: {data!r}")
            return None

    def test_load_test_graph(self):
        """Test loading data into TEST graph and print Kafka messages."""
        errors, edges, vertices = [], [], []
        self.curGraph = "TEST" + self.id_generator()
        logger.info(f"Creating graph: {self.curGraph}")
        self._conn.addGraph(self.curGraph)
        G = self._conn.graph(self.curGraph)

        vertex_file = os.path.join(BASE, "graphs", f"{self.loadgraphname}.vertices")
        logger.info(f"Loading vertices from: {vertex_file}")
        if os.path.exists(vertex_file):
            with open(vertex_file) as handle:
                for line in handle:
                    data = json.loads(line.strip())
                    id = data.get("_id", None)
                    if id is not None:
                        vertices.append(id)
                    logger.debug(f"Adding vertex: {id}")
                    G.addVertex(id, data["_label"], self.collect_fields_dict(data))
        else:
            raise FileNotFoundError(f"Vertex file not found: {vertex_file}")

        # Load edges
        edge_file = os.path.join(BASE, "graphs", f"{self.loadgraphname}.edges")
        logger.info(f"Loading edges from: {edge_file}")
        if os.path.exists(edge_file):
            with open(edge_file) as handle:
                for line in handle:
                    data = json.loads(line.strip())
                    id = data.get("_id", None)
                    if id is not None:
                        edges.append(id)
                    logger.debug(f"Adding edge: {data['_from']} -> {data['_to']}")
                    G.addEdge(
                        src=data["_from"],
                        dst=data["_to"],
                        id=id,
                        label=data["_label"],
                        data=self.collect_fields_dict(data))
        else:
            raise FileNotFoundError(f"Edge file not found: {edge_file}")

        # Verify graph data
        vertex_count_result = list(G.query().V().count())
        if not vertex_count_result or len(vertex_count_result) <= 0 or 'count' not in vertex_count_result[0]:
            raise ValueError("Invalid vertex count response")
        vertex_count = vertex_count_result[0]['count']
        assert vertex_count > 0, f"No vertices loaded into {self.curGraph}"
        edge_count_result = list(G.query().E().count())
        edge_count = edge_count_result[0]['count'] if edge_count_result else 0
        logger.info(f"Loaded {vertex_count} vertices and {edge_count} edges")
        return errors, vertices, edges


    def test_bulk_load_test_graph(self):
        """Test loading data into TEST graph and print Kafka messages."""
        errors, edges, vertices = [], [], []
        self.curGraph = "TEST" + self.id_generator()
        logger.info(f"Creating graph: {self.curGraph}")
        self._conn.addGraph(self.curGraph)
        G = self._conn.graph(self.curGraph)

        vertex_file = os.path.join(BASE, "graphs", f"{self.loadgraphname}.vertices")
        logger.info(f"Loading vertices from: {vertex_file}")
        if os.path.exists(vertex_file):
            with open(vertex_file) as handle:
                bulk = G.bulkAdd()
                for line in handle:
                    data = json.loads(line.strip())
                    id = data.get("_id", None)
                    vertices.append(id)
                    bulk.addVertex(id=id, label=data["_label"], data=self.collect_fields_dict(data))
                _ = bulk.execute()
        else:
            raise FileNotFoundError(f"Vertex file not found: {vertex_file}")

        edge_file = os.path.join(BASE, "graphs", f"{self.loadgraphname}.edges")
        logger.info(f"Loading edges from: {edge_file}")
        if os.path.exists(edge_file):
            with open(edge_file) as handle:
                bulk = G.bulkAdd()
                for line in handle:
                    data = json.loads(line.strip())
                    id = data.get("_id", None)
                    edges.append(id)
                    G.addEdge(
                        src=data["_from"],
                        dst=data["_to"],
                        id=id,
                        label=data["_label"],
                        data=self.collect_fields_dict(data))
                _ = bulk.execute()

        else:
            raise FileNotFoundError(f"Edge file not found: {edge_file}")

        # Verify graph data
        vertex_count_result = list(G.query().V().count())
        if not vertex_count_result or 'count' not in vertex_count_result[0]:
            raise ValueError("Invalid vertex count response")
        vertex_count = vertex_count_result[0]['count']
        assert vertex_count > 0, f"No vertices loaded into {self.curGraph}"
        edge_count_result = list(G.query().E().count())
        edge_count = edge_count_result[0]['count'] if edge_count_result else 0
        logger.info(f"Loaded {vertex_count} vertices and {edge_count} edges")
        return errors, vertices, edges


    def test_write_from_kafka(self, toggle_delete_method, toggle_post_method, orig_vertex_counts, orig_edge_counts):
        bulk_re = re.compile(r"^/v1/graph$")
        graph_re = re.compile(r"^/v1/graph/([^/]+)$")
        delete_vertex_re = re.compile(r"^/v1/graph/([^/]+)/vertex/([^/]+)$")
        delete_edge_re = re.compile(r"^/v1/graph/([^/]+)/edge/([^/]+)$")
        add_vertex_re = re.compile(r"^/v1/graph/([^/]+)/vertex$")
        add_edge_re = re.compile(r"^/v1/graph/([^/]+)/edge$")
        # No addSchema or add RawJson because these methods call lower level methods like bulk add and addgraph

        self.kafka_consumer = self.init_kafka_consumer()
        logger.info(f"Consuming Kafka messages from {TOPIC} topic...")
        last_message_time = time.time()
        while True:
            messages = self.kafka_consumer.poll(timeout_ms=100)
            for tp, records in messages.items():
                last_message_time = time.time()
                for msg in records:
                    if msg.value is None:
                        continue
                    headers = dict(msg.headers)
                    path = headers.get('PATH', b'').decode()
                    method = headers.get('METHOD', b'').decode()
                    if not path or not method:
                        continue

                    if method == "DELETE" and toggle_delete_method:
                        parts = path.split('/')
                        if bulk_re.match(path):
                            deleteBulk = msg.value
                            self.curGraph = deleteBulk["graph"]
                            self._conn.graph(deleteBulk["graph"]).delete(edges=deleteBulk["edges"], vertices=deleteBulk["vertices"])
                        elif delete_vertex_re.match(path):
                            parts = path.split('/')
                            graph_name = parts[-3]
                            vertex_id = parts[-1]
                            self._conn.graph(graph_name).deleteVertex(vertex_id)
                        elif delete_edge_re.match(path):
                            parts = path.split('/')
                            graph_name = parts[-3]
                            edge_id = parts[-1]
                            self._conn.graph(graph_name).deleteEdge(edge_id)
                        elif graph_re.match(path):
                            graph_name = path.split('/')[-1]
                            print("DELETE GRAPHER: ", graph_name)
                            err  = self._conn.deleteGraph(graph_name)
                    elif method == "POST" and toggle_post_method:
                        if bulk_re.match(path):
                            G = None
                            bulk = None
                            for i, elem in enumerate(msg.value.split("\n")):
                                if elem != "":
                                    elem = json.loads(elem)
                                    if i == 0:
                                        graph = elem.get("graph")
                                        self.curGraph = graph
                                        G = self._conn.graph(graph)
                                        bulk = G.bulkAdd()
                                    vertex = elem.get("vertex", None)
                                    if vertex is not None:
                                        bulk.addVertex(id=str(vertex.get("id")), label=str(vertex.get("label")), data=dict(vertex.get("data")))
                                    edge = elem.get("edge", None)
                                    if edge is not None:
                                        bulk.addEdge(src=str(edge.get("from")), dst=str(edge.get("to")), id=str(edge.get("id")), label=str(edge.get("label")), data=dict(edge.get("data")))

                            if bulk is not None:
                                err = bulk.execute()
                                assert err['errorCount'] == 0, f"Err: {err}"

                        elif add_vertex_re.match(path):
                            graph_name = path.split('/')[-2]
                            value = msg.value
                            self._conn.graph(graph_name).addVertex(
                                id=value["id"],
                                label=value["label"],
                                data=value["data"]
                            )
                        elif add_edge_re.match(path):
                            graph_name = path.split('/')[-2]
                            value = msg.value
                            self._conn.graph(graph_name).addEdge(
                                src=value["from"],
                                dst=value["to"],
                                data=value["data"],
                                id=value["id"],
                                label=value["label"]
                            )
                        elif graph_re.match(path):
                            graph_name = path.split('/')[-1]
                            err = self._conn.addGraph(graph_name)
                            print("ADD GRAPHERR: ", err)


            current_time = time.time()
            if current_time - last_message_time > (int(TIMEOUTMS) / 1000):
                break

        # Verify graph data
        if toggle_delete_method:
            try:
                G = self._conn.graph(self.curGraph)
                vertex_count_result = list(G.query().V().count())
            except requests.exceptions.HTTPError as e:
                assert "was not found" in str(e)
        else:
            G = self._conn.graph(self.curGraph)
            vertex_count_result = list(G.query().V().count())
            vertex_count = vertex_count_result[0]['count']
            assert vertex_count > 0, f"No vertices loaded into {self.curGraph}"
            edge_count_result = list(G.query().E().count())
            edge_count = edge_count_result[0]['count'] if edge_count_result else 0
            if orig_vertex_counts is not None and orig_edge_counts is not None:
                assert orig_vertex_counts == vertex_count, f"original_vertex_counts {orig_vertex_counts} != vertex_count {vertex_count}"
                assert orig_edge_counts == edge_count, f"original_edge_counts {orig_edge_counts} != edge_count {edge_count}"
            elif toggle_delete_method:
                assert vertex_count == 0 and edge_count == 0, "edges and vertices should have been deleted"
            logger.info(f"Loaded {vertex_count} vertices and {edge_count} edges")


def create_connection(server, user=None, password=None):
    """Setup connection based on credentials."""
    try:
        conn = gripql.Connection(server, user=user, password=password)
        logger.info(f"Connected to GRIP server: {server}")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to GRIP: {e}")
        return None

def main():
    conn = create_connection(GRIP_SERVER_URL, None, None)
    if not conn:
        sys.exit(1)

    manager = Manager(conn=conn, readOnly=False, server=GRIP_SERVER_URL)
    _, vertices, edges = manager.test_load_test_graph()
    manager.clean(vertices=vertices, edges=edges)
    manager.test_write_from_kafka(toggle_delete_method=False, toggle_post_method=True, orig_vertex_counts=len(vertices), orig_edge_counts=len(edges))
    manager.test_write_from_kafka(toggle_delete_method=True, toggle_post_method=False, orig_vertex_counts=None, orig_edge_counts=None)

    bulkManager = Manager(conn=conn, readOnly=False, server=GRIP_SERVER_URL)
    _, bulkvertices, bulkedges = bulkManager.test_bulk_load_test_graph()
    bulkManager.clean(vertices=bulkvertices, edges=bulkedges)
    #  Kafka is going to pickup the logs from the test before this  too, so delete everything
    bulkManager.test_write_from_kafka(toggle_delete_method=True, toggle_post_method=True, orig_vertex_counts=None, orig_edge_counts=None)

if __name__ == "__main__":
    main()
