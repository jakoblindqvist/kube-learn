from influxdb import InfluxDBClient
import sys
from requests.exceptions import ConnectionError
from datetime import datetime, timedelta
import re

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

    if flags['sum']:
        value = "sum(\"f64\")"

    if flags['rate'] and flags['nonNegRate']:
        raise ValueError("Cannot have both rate and nonNegRate set")

    if flags['rate']:
        value = "derivative(mean(\"f64\"), " + flags['rateTime'] + ")"

    if flags['nonNegRate']:
        value = "non_negative_derivative(mean(\"f64\"), " + flags['rateTime'] + ")"

    if flags['smooth']:
        value = "moving_average(" + value + ", " + str(flags['smoothLevel']) + ")"

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
def get_group_string(flags, groupTime):
    group = "time(" + groupTime + ")"

    for group_entry in flags['group']:
        group += ", " + group_entry

    return group + " fill(linear)"


"""
    Generates the influxDB query from the measure info that can be executed
"""
def generate_query(metric, times, groupTime):
    if is_dict_key_set(metric, 'name'):
        name = metric['name']
    else:
        raise ValueError("Needs to have a name")

    # Default flags
    flags = {
        'rate': False,
        'sum': False,
        'nonNegRate': False,
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
    group = get_group_string(flags, groupTime)
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

    if latest_start_time >= earliest_stop_time:
        raise ValueError("Measures don't overlap")

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

def fill_missing_values(data, groupTime):
    day_match = re.match(r"(\d+)d", groupTime)
    hour_match = re.match(r"(\d+)h", groupTime)
    minute_match = re.match(r"(\d+)m($|\d)", groupTime)
    second_match = re.match(r"(\d+)s", groupTime)
    ms_match = re.match(r"(\d+)ms", groupTime)

    days = 0
    hours = 0
    minutes = 0
    seconds = 0
    ms = 0

    if day_match:
        days = int(day_match.group(1))
    if hour_match:
        hours = int(hour_match.group(1))
    if minute_match:
        minutes = int(minute_match.group(1))
    if second_match:
        seconds = int(second_match.group(1))
    if ms_match:
        ms = int(ms_match.group(1))

    # Find lastest start and earliest stop
    earliest_start_time = datetime.now()
    latest_stop_time = data[0][0][0]
    for time_data in data:
        if time_data[0][0] < earliest_start_time:
            earliest_start_time = time_data[0][0]
        if time_data[0][-1] > latest_stop_time:
            latest_stop_time = time_data[0][-1]

    # For all values
    for time_value in data:
        # Fill values so it starts on earliest start time
        start_value = time_value[1][0]
        while time_value[0][0] > earliest_start_time:
            previous_time = time_value[0][0]
            time_value[0].insert(0, previous_time - timedelta(days = days, hours = hours, minutes = minutes, seconds = seconds, milliseconds = ms))
            time_value[1].insert(0, start_value)
        # Fill values so it stops on latest stop time
        stop_value = time_value[1][-1]
        while time_value[0][-1] < latest_stop_time:
            previous_time = time_value[0][-1]
            time_value[0].append(previous_time + timedelta(days = days, hours = hours, minutes = minutes, seconds = seconds, milliseconds = ms))
            time_value[1].append(stop_value)

    # For all values
    for time_value in data:
        current_time = earliest_start_time
        last_value = 0
        current_index = 0
        while current_time <= latest_stop_time:
            if current_time in time_value[0]:
                current_index = time_value[0].index(current_time)
                if time_value[1][current_index] == None:
                    time_value[1][current_index] = last_value
                else:
                    last_value = time_value[1][current_index]
            else:
                current_index += 1
                time_value[0].insert(current_index, current_time)
                time_value[1].insert(current_index, last_value)

            current_time += timedelta(days = days, hours = hours, minutes = minutes, seconds = seconds, milliseconds = ms)

    return data

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
    first_length = len(data[0][0])

    for i in range(first_length):
        new_value = []
        for time_value in data:
            if len(time_value[0]) != first_length:
                raise ValueError("Measurements aren't the same lengths")
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

    if not is_dict_key_set(query_configs, 'groupTime'):
        query_configs['groupTime'] = "1s"

    for metric_config in query_configs['metrics']:
        norm = False
        fill = False
        if is_dict_key_set(metric_config, 'flags'):
            if is_dict_key_set(metric_config['flags'], 'normalize'):
                norm = True
            if is_dict_key_set(metric_config['flags'], 'fill'):
                fill = True

        query = generate_query(metric_config, query_configs['times'], query_configs['groupTime'])
        query_result = execute_query(client, query)
        result, label = process_query_result(query_result)
        label = [metric_config['name'] + ": " + s for s in label]
        if fill:
            result = fill_edges(result)
        if norm:
            result = normalize_data(result)
        data += result
        labels += label

    print "Merging data"

    before = len(data[0][0])

    data = fill_missing_values(data, query_configs['groupTime'])
    data = trim_edges(data)

    print "Lost %d%% of data" % ((before - len(data[0][0])) / float(before) * 100)

    metrics, times = reorder_data(data)

    if len(metrics) == 0:
        print ""
        print "WARNING: no data was fetched"

    return {
        'data': metrics,
        'feature_names': labels,
        'times': times
    }

if __name__ == '__main__':
    print "Influx fetcher loaded"
