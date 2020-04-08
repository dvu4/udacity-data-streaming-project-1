"""Methods pertaining to loading and configuring CTA "L" station data."""
import logging
from pathlib import Path

from confluent_kafka import avro

from models import Turnstile
from models.producer import Producer


logger = logging.getLogger(__name__)


class Station(Producer):
    """Defines a single station"""

    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_key.json")

    #
    # TODO: Define this value schema in `schemas/station_value.json, then uncomment the below
    #
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_value.json")

    def __init__(self, station_id, name, color, direction_a=None, direction_b=None):
        self.name = name
        station_name = (
            self.name.lower()
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
        topic_name = f"{station_name}" # TODO: Come up with a better topic name
        super().__init__(
            topic_name,
            key_schema=Station.key_schema,
            # TODO: 
            value_schema=Station.value_schema, # TODO: Uncomment once schema is defined
            # https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/
            # p : single partition for production 
            # c : single partition for consumption
            # t : target throughput
            # choose at least max(t/p, t/c)
            # partions = max(throughput/#producers, throughput/#consumers)

            # Partitions = Max(Overall Throughput/Producer Throughput, Overall Throughput/Consumer Throughput)
            # Example from video, with 3 Producers and 5 Consumers, each operating at 10MB/s per single producer/consumer
            # partition: Max(100MBs/(3 * 10MB/s), 100MBs/(5 * 10MB/s)) = Max(2) ~= *4 partitions needed*
            # TODO: 
            num_partitions=2, # higher partition leads to higher throughput but high latency
            # TODO: 
            num_replicas=1, # replicas  shared between brokers
        )

        self.station_id = int(station_id)
        self.color = color
        self.dir_a = direction_a
        self.dir_b = direction_b
        self.a_train = None
        self.b_train = None
        self.turnstile = Turnstile(self)


    def run(self, train, direction, prev_station_id, prev_direction):
        """Simulates train arrivals at this station"""
        #
        #
        # TODO: Complete this function by producing an arrival message to Kafka
        #
        #
        #logger.info("arrival kafka integration incomplete - skipping")

        # make sure the arrival events to kafka are paired with Avro key and value schemas
        
        # look at train.py and line.py to get the properties of those instances (train and line)
        self.producer.produce(
            topic=self.topic_name,
            key={"timestamp": self.time_millis()},
            value={
                # TODO: Configure this
                "station_id" : self.station_id ,
                "train_id" : train.train_id, # to get train_id, look at `self.train_id` in train.py
                "direction" : direction,
                "line" : self.color.name, # to get the line , look at `self.color.name` in line.py
                "train_status" : train.status.name, # to get train status, look at `self.status.name` in train.py 
                "prev_station_id" : prev_station_id, 
                "prev_direction" : prev_direction
            },
        )
        logger.info(f"producing arrival event to kafka is complete")




    def __str__(self):
        return "Station | {:^5} | {:<30} | Direction A: | {:^5} | departing to {:<30} | Direction B: | {:^5} | departing to {:<30} | ".format(
            self.station_id,
            self.name,
            self.a_train.train_id if self.a_train is not None else "---",
            self.dir_a.name if self.dir_a is not None else "---",
            self.b_train.train_id if self.b_train is not None else "---",
            self.dir_b.name if self.dir_b is not None else "---",
        )

    def __repr__(self):
        return str(self)

    def arrive_a(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'a' direction"""
        self.a_train = train
        self.run(train, "a", prev_station_id, prev_direction)

    def arrive_b(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'b' direction"""
        self.b_train = train
        self.run(train, "b", prev_station_id, prev_direction)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.turnstile.close()
        super(Station, self).close()
