from __future__ import absolute_import, print_function, unicode_literals

import json
import os
import sys
import string
import random
from run_util import gripql, create_connection, Manager
import re
from kafka import KafkaConsumer
import logging
import time
import requests
from kafka import TopicPartition

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
TIMEOUTMS = 10000  # Integer for consistency


class KafkaManager(Manager):
    """Common test methods."""
    def __init__(self, conn, readOnly=False, server=None, grip_config_file_path=None):
        super().__init__(conn, readOnly, server, grip_config_file_path)
        self.loadgraphname = "swapi"
        self.kafka_consumer = self.init_kafka_consumer()

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

    def test_bulk_load_test_graph(self):
        """Test loading data into TEST graph and print Kafka messages."""
        errors, edges, vertices = [], [], []
        self.curGraph = "TEST" + self.id_generator()
        logger.info(f"Creating graph: {self.curGraph}")
        self._conn.addGraph(self.curGraph)
        logger.info("CALLING ADD GRAPH +++++++++++++++++++++++++++++++++++++++")

        G = self._conn.graph(self.curGraph)
        vertex_file = os.path.join(BASE, "graphs", f"{self.loadgraphname}.vertices")
        logger.info(f"Loading vertices from: {vertex_file}")
        if os.path.exists(vertex_file):
            with open(vertex_file) as handle:
                bulk = G.bulkAdd()
                for line in handle:
                    data = json.loads(line.strip())
                    id = data.get("_id", None)
                    if id is not None:
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
                    if id is not None:
                        edges.append(id)
                    G.addEdge(
                        src=data["_from"],
                        dst=data["_to"],
                        id=data.get("_id", None),
                        label=data["_label"],
                        data=self.collect_fields_dict(data))
                _ = bulk.execute()
        else:
            raise FileNotFoundError(f"Edge file not found: {edge_file}")

        # Verify graph data
        vertex_count_result = list(G.V().count())
        if not vertex_count_result or 'count' not in vertex_count_result[0]:
            raise ValueError("Invalid vertex count response")
        vertex_count = vertex_count_result[0]['count']
        assert vertex_count > 0, f"No vertices loaded into {self.curGraph}"
        edge_count_result = list(G.V().outE().count())
        edge_count = edge_count_result[0]['count'] if edge_count_result else 0
        logger.info(f"Loaded {vertex_count} vertices and {edge_count} edges")
        logger.info("CALLING DELETE GRAPH ------------------------------------------")
        self._conn.deleteGraph(self.curGraph)

        return errors, edges, vertices
    def test_write_from_kafka(self, toggle_delete_method, toggle_post_method, orig_vertex_counts, orig_edge_counts):
            bulk_re = re.compile(r"^/v1/graph$")
            graph_re = re.compile(r"^/v1/graph/([^/]+)$")
            delete_vertex_re = re.compile(r"^/v1/graph/([^/]+)/vertex/([^/]+)$")
            delete_edge_re = re.compile(r"^/v1/graph/([^/]+)/edge/([^/]+)$")
            add_vertex_re = re.compile(r"^/v1/graph/([^/]+)/vertex$")
            add_edge_re = re.compile(r"^/v1/graph/([^/]+)/edge$")

            created_graphs = set()  # Track created graphs to avoid duplicates
            messages = []  # Collect snapshot of messages

            logger.info(f"Capturing snapshot of Kafka messages from {TOPIC} topic...")

            # Get the current end offsets for all partitions of the topic
            partitions = self.kafka_consumer.partitions_for_topic(TOPIC)
            if not partitions:
                logger.warning(f"No partitions found for topic {TOPIC}")
                return
            topic_partitions = [TopicPartition(TOPIC, p) for p in partitions]
            end_offsets = self.kafka_consumer.end_offsets(topic_partitions)

            # Seek to the beginning of all assigned partitions
            for tp in topic_partitions:
                self.kafka_consumer.seek(tp, 0)  # Seek to offset 0 (beginning)

            messages_read = {tp: 0 for tp in topic_partitions}

            # Poll for messages until we reach the initial end offsets
            start_time = time.time()
            while True:
                message_batch = self.kafka_consumer.poll(timeout_ms=100)  # Short timeout
                if not message_batch:
                    if time.time() - start_time > TIMEOUTMS / 1000:
                        logger.info("Timeout reached while collecting initial messages")
                        break
                    continue

                for tp, records in message_batch.items():
                    for msg in records:
                        if msg.value is not None:
                            messages.append(msg)
                            messages_read[tp] += 1

                # Check if we've consumed up to the initial end offset for all partitions
                all_caught_up = True
                for tp in topic_partitions:
                    if messages_read[tp] < end_offsets[tp]:
                        all_caught_up = False
                        break

                if all_caught_up and messages:
                    break  # All initial messages consumed

            logger.info(f"Collected {len(messages)} initial Kafka messages in snapshot")

            # Unsubscribe to stop further consumption
            self.kafka_consumer.unsubscribe()

            # Process the captured messages
            for msg in messages:
                headers = dict(msg.headers)
                path = headers.get('PATH', b'').decode()
                method = headers.get('METHOD', b'').decode()
                if not path or not method:
                    continue

                logger.info(f"Processing Kafka message: PATH={path}, METHOD={method}, VALUE={msg.value}")

                if method == "DELETE" and toggle_delete_method:
                    parts = path.split('/')
                    if bulk_re.match(path):
                        deleteBulk = msg.value
                        self.curGraph = deleteBulk["graph"]
                        logger.info(f"Deleting bulk graph: {self.curGraph}")
                        self._conn.graph(deleteBulk["graph"]).delete(edges=deleteBulk["edges"], vertices=deleteBulk["vertices"])
                    elif delete_vertex_re.match(path):
                        graph_name = parts[-3]
                        vertex_id = parts[-1]
                        logger.info(f"Deleting vertex {vertex_id} from graph {graph_name}")
                        self._conn.graph(graph_name).deleteVertex(vertex_id)
                    elif delete_edge_re.match(path):
                        graph_name = parts[-3]
                        edge_id = parts[-1]
                        logger.info(f"Deleting edge {edge_id} from graph {graph_name}")
                        self._conn.graph(graph_name).deleteEdge(edge_id)
                    elif graph_re.match(path):
                        graph_name = path.split('/')[-1]
                        logger.info(f"CALLING DELETE GRAPH ------------------------------------------ for {graph_name}")
                        self._conn.deleteGraph(graph_name)
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
                                    logger.info(f"Setting self.curGraph to: {self.curGraph}")
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
                        logger.info(f"Adding vertex {value['id']} to graph {graph_name}")
                        self._conn.graph(graph_name).addVertex(
                            id=value["id"],
                            label=value["label"],
                            data=value["data"]
                        )
                    elif add_edge_re.match(path):
                        graph_name = path.split('/')[-2]
                        value = msg.value
                        logger.info(f"Adding edge {value['id']} to graph {graph_name}")
                        self._conn.graph(graph_name).addEdge(
                            src=value["from"],
                            dst=value["to"],
                            data=value["data"],
                            id=value["id"],
                            label=value["label"]
                        )
                    elif graph_re.match(path):
                        graph_name = path.split('/')[-1]
                        if graph_name not in created_graphs:
                            logger.info(f"CALLING ADD GRAPH +++++++++++++++++++++++++++++++++++++++ for {graph_name}")
                            self._conn.addGraph(graph_name)
                            created_graphs.add(graph_name)
                        else:
                            logger.info(f"Skipping duplicate ADD GRAPH for {graph_name}")

            # Verify graph data
            if toggle_post_method:
                G = self._conn.graph(self.curGraph)
                vertex_count_result = list(G.V().count())
                vertex_count = vertex_count_result[0]['count']
                assert vertex_count > 0, f"No vertices loaded into {self.curGraph}"
                edge_count_result = list(G.V().outE().count())
                edge_count = edge_count_result[0]['count'] if edge_count_result else 0
                if orig_vertex_counts is not None and orig_edge_counts is not None:
                    assert orig_vertex_counts == vertex_count, f"original_vertex_counts {orig_vertex_counts} != vertex_count {vertex_count}"
                    assert orig_edge_counts == edge_count, f"original_edge_counts {orig_edge_counts} != edge_count {edge_count}"
                    logger.info("assert statements passed")
                logger.info(f"Loaded {vertex_count} vertices and {edge_count} edges")
                self._conn.deleteGraph(self.curGraph)
                G = self._conn.graph(self.curGraph)
                try:
                    vertex_count_result = list(G.V().count())
                except Exception as e:
                    assert "was not found" in str(e)


def main():
    conn = create_connection(GRIP_SERVER_URL, None, None)
    if not conn:
        sys.exit(1)

    manager = KafkaManager(conn=conn, readOnly=False, server=GRIP_SERVER_URL)
    errors, edges, vertices = manager.test_bulk_load_test_graph()
    manager.test_write_from_kafka(toggle_delete_method=False, toggle_post_method=True, orig_vertex_counts=len(vertices), orig_edge_counts=len(edges))


if __name__ == "__main__":
    main()
