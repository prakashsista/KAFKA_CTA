"""Defines core consumer functionality"""
import logging
import confluent_kafka 
from confluent_kafka import Consumer , OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer , CachedSchemaRegistryClient
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
        
        self.broker_properties = {
                "bootstrap.servers": "localhost:9092",
                "group.id" : "2",
                "auto.offset.reset": "earliest"
        }

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
             
            schema_registry = CachedSchemaRegistryClient("http://localhost:8081")
            
            self.consumer = AvroConsumer(
                        self.broker_properties,
                        schema_registry= schema_registry
                    )
        else:
            self.consumer = Consumer(self.broker_properties)        
            logger.info(f"subscrbing topic{self.topic_name_pattern}")
        self.consumer.subscribe([self.topic_name_pattern],on_assign=self.on_assign)
        

    def on_assign(self,consumer,partitions):
                """Callback for when topic assignment takes place"""
                # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
                # the beginning or earliest
               # logger.info(f"offset is earliest {self.offset_earliest}")
                for partition in partitions:
                    if(self.offset_earliest):
                        partition.offset = OFFSET_BEGINNING
                        logger.info("partitions assigned for %s",self.topic_name_pattern)
                        self.consumer.assign(partitions)


    async def consume(self):
            """Asynchronously consumes data from kafka topic"""
            while True:
                #logger.debug("Asynchronous consumer called")
                num_results = 1
                while num_results > 0:
                    num_results = self._consume()
                await gen.sleep(self.sleep_secs)

            
    def _consume(self):
                """Polls for a message. Returns 1 if a message was received, otherwise"""
                #
                #
                # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
                # Additionally, make sure you return 1 when a message is processed, and 0 when no message
                # is retrieved.
                #
                #
                
                while True:
                    message = self.consumer.poll(1.0)
                    if message is None:
                        logger.info("no message received by consumer")
                        return 0
                    elif message.error() is not None:
                        logger.info(f"error from consumer {message.error()}")
                    else:
                        try:
                            #logger.info(message.value())
                            self.message_handler(message)
                            return 1
                        except error as e:
                            logger.info(f"Failed to unpack message {e}")
    

                  
    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()