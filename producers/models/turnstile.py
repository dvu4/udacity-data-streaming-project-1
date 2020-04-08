"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    #
    # TODO: Define this value schema in `schemas/turnstile_value.json, then uncomment the below
    #
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        #
        #
        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #
        #
        super().__init__(
            "org.chicago.cta.turnstile", # TODO: Come up with a better topic name
            key_schema=Turnstile.key_schema,
            # TODO: 
            value_schema=Turnstile.value_schema, #TODO: Uncomment once schema is defined
            # https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/
            # p : single partition for production 
            # c : single partition for consumption
            # t : target throughput
            # choose at least max(t/p, t/c)
            # TODO: 
            num_partitions=3,
            # TODO: 
            num_replicas=2
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)


    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        try:
            num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
            #logger.info("turnstile kafka integration incomplete - skipping")
            #
            #
            # TODO: Complete this function by emitting a message to the turnstile topic for the number
            # of entries that were calculated
            #
            #

            # make sure the arrival events to kafka are paired with Avro key and value schemas
            logger.info(" %s people entered this station %s ", num_entries, self.station.name)
            #
            for _ in range(num_entries):
                self.producer.produce(
                    topic=self.topic_name,
                    key={"timestamp": self.time_millis()},
                    value={
                        # TODO: Configure this
                        "station_id" : self.station.station_id,
                        "station_name" : self.station.name, 
                        "line" : self.station.color.name
                        },
                    )
        except Exception as e:
            logger.info(f"Turnstile failed to write to topic {} with exception {}".format(self.topic_name, e))
            logger.info("schema : {}".format(Turnstile.value_schema))
            logger.info("value : {}, {}, {}".format(self.station.station_id, self.station.name, self.station.color.name))






