"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])
    BROKERS_URL = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"
    SCHEMA_REGISTRY_URL = "http://127.0.0.1:8081"
    schema_registry = CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL)
    logger.info(f"Producer static initialization {schema_registry}")

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.schema_registry = Producer.schema_registry

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "bootstrap.servers": Producer.BROKERS_URL,
            "client.id": "my_producers"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            schema_registry=self.schema_registry,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )
        
        logger.info("Initiialized producer for topic: %s", self.topic_name)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        if self.topic_name in Producer.existing_topics:
            logger.info("Topic %s already exists - skipping creatiion", self.topic_name)
        else:
            logger.info("Creating topic: %s", self.topic_name)
            AdminClient(self.broker_properties).create_topics([
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    config={
                        "cleanup.policy": "delete",
                        "compression.type": "lz4",
                        "delete.retention.ms": 100,
                        "file.delete.delay.ms": 100
                    }
                )
            ])

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
        self.schema_registry.close()
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
