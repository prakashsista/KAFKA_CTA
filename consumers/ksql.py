"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests
import topic_check

logger = logging.getLogger(__name__)
KSQL_URL = "http://localhost:8088"

#
# TODO: Complete the following KSQL statements.
# TODO: For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
# TODO: For the second statment, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON

KSQL_STATEMENT = """

CREATE TABLE turnstile (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    kafka_topic = 'com.cta.turnstiles',
    value_format = 'avro',
    key = 'station_id'
);

CREATE TABLE TURNSTILE_SUMMARY
With (value_format='json')
AS
 select station_id,count(station_id) as COUNT from 
  turnstile group by station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    #print(f"executing ksql statement at {KSQL_STATEMENT}")

    resp = requests.post(
                    f"{KSQL_URL}/ksql",
            headers={"Content-Type": "application/vnd.ksql.v1+json"},
            data=json.dumps(
                    {
                        "ksql": KSQL_STATEMENT,
                        "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
                    }
                ),
            )
    ###
    # Ensure that a 2XX status code was returned
    #Check if the request status code is in 200 to 299 range
    #if resp.status_code not in range(200,299,1):
    
    resp.raise_for_status()
    
if __name__ == "__main__":
    execute_statement()
