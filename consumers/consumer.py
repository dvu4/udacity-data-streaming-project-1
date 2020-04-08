"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        # Use the Avro Consumer
        # See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=loads#confluent_kafka.avro.AvroConsumer

        self.broker_properties = {
                #
                # TODO
                "bootstrap.servers" : "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094", 
                # TODO
                "group.id": f"{self.topic_name_pattern}"
                # TODO see details for config : https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
                #  Consumer Offset is number stored in a private Kafka topic which identifies the last consumed message for a consumer
                "default.topic.config": 
                {
                    "acks": "all", 
                    "auto.offset.reset" : "earliest"
                },
        }
            

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081" 
            # running in docker
            # check confluent schema-registry on Docker Hub at https://hub.docker.com/r/confluent/schema-registry
            # Schema Registry expose port 8081 for use by the host machine
            #self.broker_properties["schema.registry.url"] = "http://schema-registry:8081/"  
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)
            #pass

        #
        #
        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        #
        #
        self.consumer.subscribe( [self.topic_name_pattern], on_assign = self.on_assign )

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        #logger.info("on_assign is incomplete - skipping")
        for partition in partitions:
            #pass
            #
            #
            # TODO
            if self.offset_earliest is True:
                partitions.offset = confluent_kafka.OFFSET_BEGINNING

            

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

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
        #
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        #
        #
        #logger.info("_consume is incomplete - skipping")
        #return 0
        try:
            message = self.consumer.poll(timeout=1.0)
            if message is not None:
                if message.error() is not None:
                    self.message_handler(message)
                    return 1
                elif message.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(message.error())
                    return 0
            else:
                logger.debug("no message received by consumer")
                return 0
        except SerializerError as e:
            logger.error(f"Message deserialization failed for {message} : {e}")
            return 0


    def close(self):
        """Cleans up any open kafka consumers"""
        #
        #
        # TODO: Cleanup the kafka consumer
        #
        #
        logger.info("Closing down consumer to commit final offsets")
        self.consumer.close()
