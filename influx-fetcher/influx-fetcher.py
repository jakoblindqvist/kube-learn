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
    Checks if a key exists and is set in a dict
"""
def is_dict_key_set(dict, test_key):
    return (test_key in dict and dict[test_key])

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
    if name[0] == '/' and name[-1] == '/':
        where = "(\"__name__\" =~ " + name + ")"
    else:
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
    Takes a Result set and converts it to an array with times and values and an array with feature names
"""
def process_query_result(query_result):
    result_tags = list(query_result.keys())

    keys = []
    values = []
    feature_names = []
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

        points = list(query_result.get_points(tags=tag_only))

        time = []
        data = []
        for time_value in points:
            time.append(datetime.fromtimestamp(time_value['time']))
            data.append(time_value['data_value'])

        values.append([time, data])
        feature_names.append(tag_string)
    return values, feature_names

"""
    Fills the edges that have None values and extrapolate with the first/last values
"""
def fill_edges(data):
    # For all values
    for time_value in data:
        searching_start = True
        start_last_index = 0
        stop_first_index = len(time_value[1])
        for i, entry in enumerate(time_value[1]):
            # Find fist value
            if searching_start and entry == None:
                start_last_index = i
            else:
                searching_start = False
            # Find last value
            if not searching_start and entry == None:
                stop_first_index = i
                break

        if start_last_index != 0:
            # Fill all entries before first value
            start_value = time_value[1][start_last_index + 1]
            for i in range(start_last_index + 1):
                time_value[1][i] = start_value

        if stop_first_index != len(time_value[1]):
            # Fill all entries after last value
            stop_value = time_value[1][stop_first_index - 1]
            for i in range(stop_first_index, len(time_value[1])):
                time_value[1][i] = stop_value

    return data

"""
    Normalizes so the data will be in the range 0, 1
"""
def normalize_data(data):
    max_value = -sys.maxint - 1
    min_value = sys.maxint
    for time_value in data:
        for value in time_value[1]:
            if value > max_value:
                max_value = value
            if value < min_value:
                min_value = value

    for time_value in data:
        norm_data = []
        for value in time_value[1]:
            normalized = (value - min_value) / float(max_value - min_value)
            norm_data.append(normalized)
        time_value[1] = norm_data

    return data

"""
    Removes datapoints at the edge so the samples always starts and ends at the same time
"""
def trim_edges(data):
    result = []

    # Find lastest start and earliest stop
    latest_start_time = data[0][0][0]
    earliest_stop_time = datetime.now()
    for time_data in data:
        if time_data[0][0] > latest_start_time:
            latest_start_time = time_data[0][0]
        if time_data[0][-1] < earliest_stop_time:
            earliest_stop_time = time_data[0][-1]

    # Trim edges so all data starts and stops on same time
    for time_data in data:
        new_data = []
        new_time = []
        for i, time in enumerate(time_data[0]):
            if time >= latest_start_time and time <= earliest_stop_time:
                new_time.append(time)
                new_data.append(time_data[1][i])

        result.append([new_time, new_data])

    return result


"""
    Reorders the data from:
    [
      [
        [t1, t2, ...],
        [m1_1, m1_2, ...]
      ], [
        [t1, t2, ...],
        [m2_1, m2_2, ...]
      ], ...
    ]

    to:
    [
      [m1_1, m2_1, ...],
      [m1_2, m2_2, ...],
      ...
    ]
    and
    [t1, t2, ...]
"""
def reorder_data(data):
    result = []
    time = []
    for i in range(len(data[0][0])):
        new_value = []
        for time_value in data:
            new_value.append(time_value[1][i])
        result.append(new_value)
        time.append(data[0][0][i])
    return result, time

"""
    Returns metrics that is specified in the query config from the database that's specified in the influx config
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
        fill = False
        if is_dict_key_set(metric_config, 'flags'):
            if is_dict_key_set(metric_config['flags'], 'normalize'):
                norm = True
            if is_dict_key_set(metric_config['flags'], 'fill'):
                fill = True

        query = generate_query(metric_config, query_configs['times'])
        query_result = execute_query(client, query)
        result, label = process_query_result(query_result)
        if fill:
            result = fill_edges(result)
        if norm:
            result = normalize_data(result)
        data += result
        labels += label

    data = trim_edges(data)
    metrics, times = reorder_data(data)

    return {
        'data': metrics,
        'feature_names': labels,
        'times': times
    }

if __name__ == '__main__':
    print "Influx fetcher loaded"
