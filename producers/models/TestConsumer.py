# Please complete the TODO items in the code

import asyncio
from dataclasses import asdict, dataclass, field
import json
import random

from confluent_kafka import avro, Consumer, Producer
from confluent_kafka.avro import AvroConsumer, AvroProducer, CachedSchemaRegistryClient
from faker import Faker


faker = Faker()

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BROKER_URL = "PLAINTEXT://localhost:9092"


@dataclass
class ClickAttribute:
    element: str = field(default_factory=lambda: random.choice(["div", "a", "button"]))
    content: str = field(default_factory=faker.bs)

    @classmethod
    def attributes(self):
        return {faker.uri_page(): ClickAttribute() for _ in range(random.randint(1, 5))}


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))
    attributes: dict = field(default_factory=ClickAttribute.attributes)

    #
    # TODO: Load the schema using the Confluent avro loader
    #       See: https://github.com/confluentinc/confluent-kafka-python/blob/master/confluent_kafka/avro/load.py#L23
    #
    schema = avro.loads("""{
"name": "arrival.value",
"namespace": "com.Udacity",
"type": "record",
"fields": [
    
        { "name": "station_id", "type": "int"},
        { "name": "train_id", "type": "string"},
        { "name": "direction", "type": "string"},
        { "name": "line", "type": "string"},
        { "name": "train_status", "type": "string"},
        { "name": "prev_station_id", "type": ["null", "int"]},
        { "name": "prev_direction", "type": ["null", "string"]}
    ]
}""")


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    #
    # TODO: Create a CachedSchemaRegistryClient. Use SCHEMA_REGISTRY_URL.
    #       See: https://github.com/confluentinc/confluent-kafka-python/blob/master/confluent_kafka/avro/cached_schema_registry_client.py#L47
    #
    schema_registry = CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL)

    #
    # TODO: Replace with an AvroProducer.
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=loads#confluent_kafka.avro.AvroProducer
    #
    p = AvroProducer({"bootstrap.servers": BROKER_URL},
                     schema_registry=schema_registry)
    while True:
        #
        # TODO: Replace with an AvroProducer produce. Make sure to specify the schema!
        #       Tip: Make sure to serialize the ClickEvent with `asdict(ClickEvent())`
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=loads#confluent_kafka.avro.AvroProducer
        #
        p.produce(
            topic=topic_name,
            value=asdict(ClickEvent()),
            # TODO: Supply schema
            value_schema=ClickEvent.schema
        )
        await asyncio.sleep(1.0)


async def consume(topic_name):
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "2",
                     "auto.offset.reset": "earliest"})
    print(topic_name)
    c.subscribe([topic_name])
    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            try:
                #print(message.value())
                data = json.loads(message.vlaue())
                print(f"Hello data" + data)
            except KeyError as e:
                print(f"Failed to unpack message {e}")
        await asyncio.sleep(1.0)

async def Avroconsume(topic_name):
    """Consumes data from the Kafka Topic"""
    #
    # TODO: Create a CachedSchemaRegistryClient
    schema_registry = CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL)

    #
    # TODO: Use the Avro Consumer
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=loads#confluent_kafka.avro.AvroConsumer
    #
    c = AvroConsumer({"bootstrap.servers": BROKER_URL, "group.id": "3",
                     "auto.offset.reset": "earliest"},schema_registry=schema_registry)
    
    
    print(topic_name)
    #c1.subscribe([topic_name])
    c.subscribe([topic_name])
    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            try:
                data = message.value()
                print(message.topic())
            except KeyError as e:
                print(f"Failed to unpack message {e}")
        await asyncio.sleep(1.0)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    try:
        asyncio.run(produce_consume("com.udacity.lesson3.exercise4.clicks"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(Avroconsume("^com.cta.stations.arrivals.*$"))#okcle
    #t2 = asyncio.create_task(Avroconsume("com.cta.turnstiles"))#ok
    #t3 = asyncio.create_task(consume("TURNSTILE_SUMMARY"))#Binary
    #t4 = asyncio.create_task(Avroconsume("org.cta.weather.info"))#ok
    #t5 = asyncio.create_task(consume("org.chicago.cta.stations"))#ok
    #t6=asyncio.create_task(consume("org.chicago.cta.transformedstations"))
    
    #check all topics
    await t1
    #await t2
    #await t3
    #await t4
    #await t5
    #await t6                             


if __name__ == "__main__":
    main()
