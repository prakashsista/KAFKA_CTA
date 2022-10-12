"""Contains functionality related to Lines"""
import json
import logging
import re

from models import Line


logger = logging.getLogger(__name__)


class Lines:
    """Contains all train lines"""

    def __init__(self):
        """Creates the Lines object"""
        self.red_line = Line("red")
        self.green_line = Line("green")
        self.blue_line = Line("blue")

    def process_message(self, message):
        """Processes a station message"""
        
        isTrainArrived = re.search("^com.cta.stations.arrivals.*$", message.topic())
        #print(message.topic())
                
        if isTrainArrived:
            data = message.value()
            logger.info("Inside train arrival lines handler")
            try:
                if data["line"] == "green":
                    self.green_line.process_message(message)
                elif data["line"] == "red":
                    self.red_line.process_message(message)
                elif data["line"] == "blue":
                    self.blue_line.process_message(message)
                else:
                    logger.debug("discarding unknown line msg %s", value["line"])    
            except Exception as ex: 
                logger.info(f"Error in unpacking the message {ex}")
            
        elif(message.topic()=="org.chicago.cta.transformedstations"):
            data = json.loads(message.value())
            #print(data)
            try:
                if data["line"] == "green":
                    self.green_line.process_message(message)
                elif data["line"] == "red":
                    self.red_line.process_message(message)
                elif data["line"] == "blue":
                    self.blue_line.process_message(message)
                else:
                    logger.debug("discarding unknown line msg %s", value["line"])
            except Exception as ex:
                    logger.info(f"Error in unpacking the message {ex}")
        elif message.topic() == "TURNSTILE_SUMMARY" :
            self.green_line.process_message(message)
            self.red_line.process_message(message)
            self.blue_line.process_message(message)
        else:
            logger.info("ignoring non-lines message %s", message.topic())