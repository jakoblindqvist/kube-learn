from influxdb import InfluxDBClient
import sys
from requests.exceptions import ConnectionError
from datetime import datetime

"""
    Class to store influxDB configuration
"""


class InfluxConfig:
    def __init__(self, ip="localhost", port=8086, user="prom", password="prom", db="prometheus"):
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


def get_where_string(name, flags, times):
    where = "(\"__name__\" = '" + name + "')"

    for where_entry in flags['where']:
        where += " AND (" + where_entry + ")"

    if is_dict_key_set(times, 'startTime'):
        where += " AND (time >= " + times['startTime'] + ")"

    if is_dict_key_set(times, 'stopTime'):
        where += " AND (time <= " + times['stopTime'] + ")"

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


def generate_query(metric, times):
    if is_dict_key_set(metric, 'name'):
        name = metric['name']
    else:
        raise ValueError("Needs to have a name")

    # Default flags
    flags = {
        'rate': False,
        'rateTime': "1s",
        'where': [],
        'group': [],
        'smooth': False,
        'smoothLevel': 5
    }

    for key in metric['flags'].keys():
        if key in flags:
            flags[key] = metric['flags'][key]

    value = get_value_string(flags)
    where = get_where_string(name, flags, times)
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

def is_dict_key_set(dict, test_key):
    return (test_key in dict and dict[test_key])
"""

"""
def get_metrics(query_configs, influx_config):
    client = get_DB_client(influx_config.ip, influx_config.port,
                           influx_config.user, influx_config.password, influx_config.db)
    data = []
    labels = []
    norm = False
    if not is_dict_key_set(query_configs, 'metrics'):
        raise ValueError("Query config needs to have some metrics")

    if not is_dict_key_set(query_configs, 'times'):
        query_configs['times'] = {}

    for metric_config in query_configs['metrics']:
        norm = False
        if is_dict_key_set(metric_config, 'flags'):
            if is_dict_key_set(metric_config['flags'], 'normalize'):
                norm = True

        query = generate_query(metric_config, query_configs['times'])
        query_result = execute_query(client, query)
        result, label = process_query_result(query_result)
        if norm:
            result = normalize_data(result)
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
    TODO
"""
def normalize_data(data):
    max_abs_value = 0
    for values in data:
        for value in values[1]:
            if abs(value) > max_abs_value:
                max_abs_value = float(abs(value))

    for values in data:
        norm_data = []
        for value in values[1]:
            normalized = value / max_abs_value
            norm_data.append(normalized)
        values[1] = norm_data

    return data

"""
    TODO
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
            tag_string += tag_key + ": " + tag_only[tag_key]
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
    conf = InfluxConfig(ip="192.168.104.186")
    metric = {
        'metrics': [
            {
                'name': 'node_cpu',
                'flags': {
                    'rate': True,
                    'group': ["instance"]
                }
            }, {
                'name': 'istio_request_count',
                'flags': {
                    'rate': True,
                    'group': ["destination_service", "source_service"],
                    'normalize': True
                }
            }
        ],
        'times': {
            'startTime': "now() - 2m"
        }
    }
    print get_metrics(metric, conf)
    #get_metrics(metric, conf)

    return 0

#if __name__ == '__main__':
#    main()
