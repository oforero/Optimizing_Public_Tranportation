"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient

from typing import List, Optional

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""
    BROKERS_URL = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"
    SCHEMA_REGISTRY_URL = "http://127.0.0.1:8081"

    def __init__(
        self,
        message_handler,
        group_id, 
        topics: List[str] = None,
        topics_name_pattern: Optional[str] = None,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        if topics_name_pattern is None and (topics is None or len(topics) == 0):
            raise ValueError("Either topics or topic_name_pattern should be passed")
        if topics_name_pattern is not None and (topics is not None and len(topics) != 0):
            raise ValueError("Only one of topics or topic_name_pattern should be passed")

        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest
        if offset_earliest:
            self.offset = "earliest"
        else:
            self.offset = "latests"

        self.broker_properties = {
            'bootstrap.servers': KafkaConsumer.BROKERS_URL,
            'group.id': group_id,
            'default.topic.config': {'auto.offset.reset': self.offset}}

        if is_avro is True:
            self.broker_properties["schema.registry.url"] = KafkaConsumer.SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        if topics_name_pattern is not None:
            # This version of the Kafka client do not use separate parameters for patterns
            self.consumer.subscribe(topics=[topics_name_pattern], on_assign=self.on_assign)
            self.topics=[topics_name_pattern]

        if topics is not None:
            self.consumer.subscribe(topics=topics, on_assign=self.on_assign) 
            self.topics=topics   
        
        logger.info("Initialized consumer for topic: %s\nProperites: %s", self.topics, self.broker_properties)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        if self.offset_earliest:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING
        consumer.assign(partitions)
        logger.info("partitions assigned for %s", self.topics)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        #
        # Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        try:
            message = self.consumer.poll(1.0)    
            if message is None:
                return 0
            else:
                self.message_handler(message)
                return 1
        except SerializerError as er:
            logger.error("Deserialization error on topic (%s): %s", self.topics, er)        
            return 0

        except Exception as ex:
            logger.error("Unhandled exception in topic (%s):\nmessage: %s\nexception:%s", self.topics, message, ex)
            return 0


    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()