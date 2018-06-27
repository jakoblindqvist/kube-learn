import datetime
import getopt
import sys
from pickle import dump

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import requests
from influxdb import InfluxDBClient

def getInfluxData(ip, measurement, rate=False):
    port        = 8086
    # measurement = "node_cpu_norm"
    groups      = "instance"
    value       = "f64"
    rateTime    = "1s"
    whereAdd    = ""
    graphic     = False
    array       = True
    pickle      = True
    fill        = False
    smooth      = True

    keyValue = value.split(',')[0] # TODO fix to use all values

    # Create group string
    groups = "time(" + rateTime + "), " + groups
    if rate:
        value = "moving_average(derivative(sum(" + value +"), " + rateTime + "), 5)"
        # keyValue = "derivative"
        pass
    else:
        value = "moving_average(sum(" + value +"), 5)"
        keyValue = "moving_average"
        pass
    keyValue = "moving_average"

    
    query = "SELECT " + value + " FROM \"_\" WHERE (\"__name__\" = \'" + measurement + "\' " + whereAdd + ") AND time >= now() - 3h GROUP BY " + groups # TODO Change "time >= 1529408612543ms AND time <= 1529411212321ms" to "time >= now() - 3h"
    try:
        client = InfluxDBClient(ip, port, 'prom', 'prom', 'prometheus')
        result = client.query(query, epoch='s')
    except requests.exceptions.ConnectionError as error:
        print >> sys.stderr, ("Error connecting to database")
        exit(1)
    
    result_tags = list(result.keys())
    
    keys = []
    values = []
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
    
        get = list(result.get_points(tags=tag_only))
    
        x = []
        y = []
        for value in get:
            time = datetime.datetime.fromtimestamp(value['time'])
            x.append(time)
            y.append(value[keyValue])
    
        values += [[x, y]]
    
    if fill:
        #Fill missing values
        for value in values:
            # For all data
            for time in value[0]:
                # For all times in data
                for other in values:
                    # For all other data
                    if not time in other[0]:
                        other[0].append(time)
                        other[1].append(0)
                        other[0], other[1] = (list(t) for t in zip(*sorted(zip(other[0], other[1]))))

    if array or pickle:
        dumper = []
        for i,_ in enumerate(values[0][0]):
            tmp = []
            for value in values:
                tmp.append(value[1][i])
            dumper.append(tmp)

        if array:
            pass
            #print dumper
        else:
            pass
            #dump(dumper, file=sys.stdout)
    return dumper
