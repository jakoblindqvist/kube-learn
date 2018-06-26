from influxdb import InfluxDBClient
import sys
from requests.exceptions import ConnectionError
from datetime import datetime

"""
    Class to store influxDB configuration
"""
class InfluxConfig:
    def __init__(self, ip = "localhost", port = 8086, user = "prom", password = "prom", db = "prometheus"):
        self.ip = ip
        self.port = port
        self.user = user
        self.password = password
        self.db = db

"""
    Executes a query and return the result
"""
def execute_query(client, query):
    print "Querying: " + query
    return client.query(query, epoch='s')

"""
    Creates the SELECT part of the query
"""
def get_value_string(flags):
    value = "mean(f64)"

    if flags['rate']:
        value = "derivative(mean(f64), " + flags['rateTime'] + ")"

    if flags['smooth']:
        value = "moving_average(" + value + ", " + flags['smoothLevel'] + ")"

    return value

"""
    Create the WHERE part of the query
"""
def get_where_string(name, flags):
    where = "(__name__ = " + name + ")"

    for where_entry in flags['where']:
        where += " AND (" + where_entry + ")"

    if flags['startTime']:
        where += " AND (time >= " + flags['startTime'] + ")"

    if flags['stopTime']:
        where += " AND (time <= " + flags['stopTime']+ ")"

    return where

"""
    Create the GROUP BY part of the query
"""
def get_group_string(flags):
    group = "time(1s)"

    for group_entry in flags['group']:
        group += ", " + group_entry

    return group + ", fill(linear)"

"""
    Generates the influxDB query from the measure info that can be executed
"""
def generate_query(metric):
    name = metric['name']

    # Default flags
    flags = {
        'rate': False,
        'rateTime': "1s",
        'where': [],
        'group': [],
        'smooth': False,
        'smoothLevel': 5,
        'startTime': "",
        'stopTime': "",
    }


    for key in metric['flags'].keys():
        if key in flags:
            flags[key] = metric['flags'][key]

    value = get_value_string(flags)
    where = get_where_string(name, flags)
    group = get_group_string(flags)
    return "SELECT " + value + " AS data_value FROM _ WHERE (" + where + ") GROUP BY " + group

"""
    Creates a DB client that can be used to execute a query
"""
def get_DB_client(ip, port, user, password, db):
    try:
        client = InfluxDBClient(ip, port, user, password, db)
    except ConnectionError as error:
        print >> sys.stderr, ("Error connecting to database")
        exit(1)

    return client

"""

"""
def get_metrics(metrics, influx_config):
    client = get_DB_client(influx_config.ip, influx_config.port, influx_config.user, influx_config.password, influx_config.db)
    data = []
    for metric in metrics:
        query = generate_query(metric)
        query_result = execute_query(client, query)
        data.append(process_query_result(query_result))

    return reorder_data(data)

"""
"""
def process_query_result(query_result):
    result_tags = list(query_result.keys())

    keys = []
    values = []
    for tag in result_tags:
        tag_only = tag[1]
        tag_keys = list(tag_only.keys())
        for i, tag_key in enumerate(tag_keys):
            if tag_key not in keys:
                keys.append(tag_key)

        get = list(query_result.get_points(tags=tag_only))

        x = []
        y = []
        for value in get:
            time = datetime.fromtimestamp(value['time'])
            x.append(time)
            y.append(value['data_value'])

        values += [[x, y]]
    return values

"""
"""
def reorder_data(data):
    return data

def main():
    conf = InfluxConfig(ip = "192.168.104.186")
    metric = [
        {
            'name': 'node_cpu',
            'flags': {
                'rate': True,
                'group': ["instance"],
                'startTime': "now() - 1h"
            }
        }
    ]
    print get_metrics(metric, conf)

    return 0

if __name__ == '__main__':
    main()