"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse

import requests

from models.producer import Producer


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    # An enumeration is a set of symbolic names (members) bound to unique, constant values. 
    # the output of IntEnum is a list of symbolic names 
    # for this example, status is a list a symbolic names weather conditions :
    # for i in status:
    #    print(i)
    # status.sunny
    # status.partly_cloudy
    # status.cloudy
    # status.windy
    # status.precipitation


    # for i in status:
    #    print(i.name)
    # sunny
    # partly_cloudy
    # cloudy
    # windy
    # precipitation

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://localhost:8082"

    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        #
        #
        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #
        #
        super().__init__(
            "org.chicago.cta.weather", # TODO: Come up with a better topic name
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            # https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/
            # p : single partition for production 
            # c : single partition for consumption
            # t : target throughput
            # choose at least max(t/p, t/c)
            # partions = max(throughput/#producers, throughput/#consumers)

            # check part 10 lesson 2 for more detail
            # Partitions = Max(Overall Throughput/Producer Throughput, Overall Throughput/Consumer Throughput)
            # Example from video, with 3 Producers and 5 Consumers, each operating at 10MB/s per single producer/consumer
            # partition: Max(100MBs/(3 * 10MB/s), 100MBs/(5 * 10MB/s)) = Max(2) ~= *4 partitions needed*
            # TODO: 
            num_partitions=3, # higher partition leads to higher throughput but high latency
            # TODO: 
            num_replicas=1, # replicas  shared between brokers
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        #
        # TODO: Define this value schema in `schemas/weather_value.json
        #
        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        # set temperature changes according to the months
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        #
        #
        # TODO: Complete the function by posting a weather event to REST Proxy. Make sure to
        # specify the Avro schemas and verify that you are using the correct Content-Type header.
        #
        #
        #logger.info("weather kafka proxy integration incomplete - skipping")

        #resp = requests.post(
        #    #
        #    #
        #    # TODO: What URL should be POSTed to?
        #    #
        #    #
        #    f"{Weather.rest_proxy_url}/TODO",
        #    #
        #    #
        #    # TODO: What Headers need to bet set?
        #    #
        #    #
        #    headers={"Content-Type": "TODO"},
        #    data=json.dumps(
        #        {
        #            #
        #            #
        #            # TODO: Provide key schema, value schema, and records
        #            #
        #            #
        #        }
        #    ),
        #)
        #resp.raise_for_status()

        logger.info("weather kafka proxy integration is in progress")
        resp = requests.post(
            # TODO: What URL should be POSTed to?   f"{Weather.rest_proxy_url}/TODO",
            f"{Weather.rest_proxy_url}/topics/{self.topic_name}",  
            # TODO: What Headers need to bet set?
            # Content-Type is in the format : application/vnd.kafka[.embedded_format].[api_version]+[serialization_format]
            # embedded_format : binary, json, or avro
            # api_version for REST Proxy :  v2
            # serialization_format : json
            headers = {"Content-Type" : "application/vnd.kafka.avro.v2+json"},
            # TODO: Update the below payload to include the Avro Schema string
            # See: https://docs.confluent.io/current/kafka-rest/api.html#post--topics-(string-topic_name)
            # Ensure that events emitted to REST Proxy are paired with the Avro key and value schemas

            # please check weather_value.json for the Avro key/value schemas
            # {"type" : "record", 
            # "name" : "weather.value",
            #   "namespace" : "com.udacity",
            #   "fields" : 
            #   [
            #     {"name": "temperature", "type" : "float"},
            #     {"name": "status", "type" : "string"}   
            #   ]
            # }
            data = json.dumps(
                {
                # TODO: Provide key schema, value schema, and records
                "key_schema"  : json.dumps(Weather.key_schema),
                "value_schema": json.dumps(Weather.value_schema),
                "records": 
                    [
                        {
                            "key"   : {"timestamp": self.time_millis()},
                            "value" : {
                                "temperature" : self.temp,
                                "status"      : self.status.name
                            }
                        }
                    ]
                }
            ),
        )
        resp.raise_for_status()

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
