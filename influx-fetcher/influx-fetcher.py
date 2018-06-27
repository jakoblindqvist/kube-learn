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
    value = "mean(\"f64\")"

    if flags['rate']:
        value = "derivative(mean(\"f64\"), " + flags['rateTime'] + ")"

    if flags['smooth']:
        value = "moving_average(" + value + ", " + flags['smoothLevel'] + ")"

    return value

"""
    Create the WHERE part of the query
"""
def get_where_string(name, flags):
    where = "(\"__name__\" = '" + name + "')"

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

    return group + " fill(linear)"

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
    return "SELECT " + value + " AS \"data_value\" FROM \"_\" WHERE (" + where + ") GROUP BY " + group

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
    labels = []
    for metric in metrics:
        query = generate_query(metric)
        query_result = execute_query(client, query)
        result, label = process_query_result(query_result)
        data += result
        labels += label

    data = trim_edges(data)

    #data = fill_holes(data)
    metrics, times = reorder_data(data)

    return {
        'metrics': metrics,
        'labels': labels,
        'times': times
    }

"""
"""
def process_query_result(query_result):
    result_tags = list(query_result.keys())

    keys = []
    values = []
    labels = []
    for tag in result_tags:
        tag_only = tag[1]
        tag_keys = list(tag_only.keys())
        value_string = ""
        tag_string = ""
        for i, tag_key in enumerate(tag_keys):
            tag_string += tag_key + ": " +  tag_only[tag_key]
            value_string += tag_only[tag_key]

            if tag_key not in keys:
                keys.append(tag_key)

            if (i + 1) < len(tag_keys):
                tag_string += " and "
                value_string += " and "

        get = list(query_result.get_points(tags=tag_only))

        time = []
        data = []
        for value in get:
            time.append(datetime.fromtimestamp(value['time']))
            data.append(value['data_value'])

        values.append([time, data])
        labels.append([value_string, tag_string])
    return values, labels



"""
    TODO
"""
#def fill_holes(data):
#    #Fill missing values
#    for value in data:
#        # For all data
#        for time in value[0]:
#            # For all times in data
#            for other_value in data:
#                # For all other data
#                if not time in other_value[0]:
#                    other_value[0].append(time)
#                    other_value[1].append(0)
#                    other_value[0], other_value[1] = (list(t) for t in zip(*sorted(zip(other_value[0], other_value[1]))))
#    return data

def trim_edges(data):
    result = []

    # Find lastest start
    latest_start_time = data[0][0][0]
    for value in data:
        if value[0][0] > latest_start_time:
            latest_start_time = value[0][0]

    # Find earliest stop
    earliest_stop_time = datetime.now()
    for value in data:
        if value[0][-1] < earliest_stop_time:
            earliest_stop_time = value[0][-1]

    # Trim edges so all data starts and stops on same time
    for value in data:
        new_data = []
        new_time = []
        for i, time in enumerate(value[0]):
            if time >= latest_start_time and time <= earliest_stop_time:
                new_time.append(time)
                new_data.append(value[1][i])

        result.append([new_time, new_data])

    return result

"""
    TODO
"""
def reorder_data(data):
    result = []
    time = []
    for i in range(len(data[0][0])):
        tmp = []
        for value in data:
            tmp.append(value[1][i])
        result.append(tmp)
        time.append(value[0][i])
    return result, time

def main():
    conf = InfluxConfig(ip = "192.168.104.186")
    metric = [
        {
            'name': 'node_cpu',
            'flags': {
                'rate': True,
                'group': ["instance"],
                'startTime': "now() - 2m"
            }
        }
    ]
    print get_metrics(metric, conf)
    #get_metrics(metric, conf)

    return 0

if __name__ == '__main__':
    main()