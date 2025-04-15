from __future__ import absolute_import, print_function, unicode_literals

import json
import os
import sys
import string
import random
import gripql
from kafka import KafkaConsumer
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define BASE directory
BASE = os.path.dirname(os.path.abspath(__file__))

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

    def clean(self):
        """Delete current graph if not in readOnly mode."""
        if self.readOnly is False and self.curGraph != "":
            logger.info(f"Deleting graph: {self.curGraph}")
            self._conn.deleteGraph(self.curGraph)
            self.curGraph = ""

    def id_generator(self, size=6, chars=string.ascii_uppercase + string.digits):
        """Random 6 alphanumeric string."""
        return ''.join(random.choice(chars) for _ in range(size)).lower()

    def init_kafka_consumer(self):
        """Initialize Kafka consumer for gripHistory topic."""
        try:
            consumer = KafkaConsumer(
                'gripHistory',
                bootstrap_servers=['localhost:9092'],
                security_protocol='SASL_PLAINTEXT',
                sasl_mechanism='PLAIN',
                sasl_plain_username='admin',
                sasl_plain_password='adminpassword',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='test-consumer-group',
                value_deserializer=lambda x: self.deserialize_message(x)
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
        errors = []
        try:
            # Create TEST graph
            self.curGraph = "TEST" + self.id_generator()
            logger.info(f"Creating graph: {self.curGraph}")
            self._conn.addGraph(self.curGraph)
            G = self._conn.graph(self.curGraph)

            # Load vertices
            graph_name = "swapi"
            vertex_file = os.path.join(BASE, "graphs", f"{graph_name}.vertices")
            logger.info(f"Loading vertices from: {vertex_file}")
            if os.path.exists(vertex_file):
                with open(vertex_file) as handle:
                    for line in handle:
                        data = json.loads(line.strip())
                        logger.debug(f"Adding vertex: {data['_id']}")
                        G.addVertex(data["_id"], data["_label"], self.collect_fields_dict(data))
            else:
                raise FileNotFoundError(f"Vertex file not found: {vertex_file}")

            # Load edges
            edge_file = os.path.join(BASE, "graphs", f"{graph_name}.edges")
            logger.info(f"Loading edges from: {edge_file}")
            if os.path.exists(edge_file):
                with open(edge_file) as handle:
                    for line in handle:
                        data = json.loads(line.strip())
                        logger.debug(f"Adding edge: {data['_from']} -> {data['_to']}")
                        G.addEdge(
                            src=data["_from"],
                            dst=data["_to"],
                            id=data.get("_id", None),
                            label=data["_label"],
                            data=self.collect_fields_dict(data)
                        )
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

            # Consume Kafka messages
            if self.kafka_consumer:
                 logger.info("Consuming Kafka messages from gripHistory topic...")
                 vertex_message_count = 0
                 timeout = time.time() + 10  # Set the 10-second timeout

                 while time.time() < timeout:
                     messages = self.kafka_consumer.poll(timeout_ms=100)  # Check for new messages every 100ms

                     if not messages:
                         time.sleep(0.1)  # Small delay if no messages to avoid busy-waiting
                         continue

                     for tp, records in messages.items():
                         for record in records:
                             msg_value = record.value
                             if msg_value is None:
                                 continue
                             logger.info(f"Received Kafka message: {msg_value}")
                             # Check if it's a vertex message (has _id, _label)
                             vertex_message_count += 1

                 logger.info("Timeout reached. Closing Kafka consumer.")
                 self.kafka_consumer.close()

                 logger.info(f"Received {vertex_message_count} vertex messages")
                 assert vertex_message_count > 182, "Not enough Kafka Logs coming back to match data loaded"


            return errors

        except Exception as e:
            logger.error(f"Test error: {e}", exc_info=True)
            return [f"Test failed: {str(e)}"]

        finally:
            self.clean()

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
    # Configuration
    server_url = "http://localhost:8201"
    user = None
    password = None

    # Create connection
    conn = create_connection(server_url, user, password)
    if not conn:
        sys.exit(1)

    manager = Manager(conn=conn, readOnly=False, server=server_url)

    result = manager.test_load_test_graph()

    if not result:
        print("Test passed successfully!")
    else:
        print("Test failed with errors:")
        for error in result:
            print(f"  - {error}")
        sys.exit(1)

if __name__ == "__main__":
    main()
