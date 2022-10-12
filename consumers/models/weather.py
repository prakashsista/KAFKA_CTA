"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)

CONTENT_TYPE = "application/vnd.kafka.avro.v2+json"

class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        if(message.topic()=="org.cta.weather.info"):
            msg = message.value()
            #logger.debug(f"Reading the weather message")
            self.temperature= msg["temperature"]
            self.status=  msg["status"]
