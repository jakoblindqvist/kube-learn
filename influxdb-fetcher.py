import datetime
import getopt
import sys
from pickle import dump

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import requests
from influxdb import InfluxDBClient


def usage():
    print("Usage: python " + sys.argv[0] + " -m <measurement> -g <group>")
    print("    -m --measurement: The measurement to fetch (required)")
    print("    -g --group: Which tags to group by separated by comma (required)")
    print("    -i --ip: The ip to the DB. default: localhost")
    print("    -p --port: The port that the DB is using. default: 8086")
    print("    -v --value: The values to fetch. default: f64")
    print("    -w --where: To add to the where string")
    print("    -r --rate: Specify if the data should be \"ratified\"")
    print("       --rate-time: If rate is specified, change how long the rate is. default: 1s")
    print("    -t --text: Prints the output to stdout instead of a plot")
    print("    -a --array: Prints the output as an python array")
    print("    -p --pickle: Use pickle to serialize the data and print to stdout")
    print("    -f --fill: Fill missing datapoints so all metrics have same amount of data")
    print("    -h --help: Displays this text")
    print("    -s --smooth: Specify if the data should use a moving average")

try:
    opts, args = getopt.getopt(sys.argv[1:], 'hrtapfm:g:v:w:i:p:s', ["help", "rate", "text", "array", "pickle", "fill", "measurement=", "group=", "value=", "rate-time=", "where=", "ip=", "port=", "smooth"])
except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(1)

ip= "localhost"
port=8086
measurement = ""
groups = ""
value = "f64"
rateTime = "1s"
whereAdd = ""
rate = False
graphic = True
array = False
pickle = False
fill = False
smooth = False
for o, a in opts:
    if o in ('-m', '--measurement'):
        measurement = a
    elif o in ('-g', '--group'):
        groups = a
    elif o in ('-i', '--ip'):
        ip = a
    elif o in ('-p', '--port'):
        port = int(a)
    elif o in ('-v', '--value'):
        value = a
    elif o in ('-w', '--where'):
        whereAdd = a
    elif o == 'rate-time':
        rateTime = a
    elif o in ('-t', '--text'):
        graphic = False
    elif o in ('-a', '--array'):
        array = True
    elif o in ('-p', '--pickle'):
        pickle = True
    elif o in ('-f', '--fill'):
        fill = True
    elif o in ('-r', '--rate'):
        rate = True
    elif o in ('-s', '--smooth'):
        smooth = True
    elif o in ("-h", "--help"):
        usage()
        exit()

if not (measurement and groups):
    print >> sys.stderr, ("ERROR: measurement and group is mandatory parameters")
    usage()
    exit(1)

keyValue = value.split(',')[0] # TODO fix to use all values

# Create group string
if rate or smooth:
    groups = "time(" + rateTime + "), " + groups

if rate:
    value = "derivative(sum(" + value +"), " + rateTime + ")"
    keyValue = "derivative"

if smooth:
    if rate:
        value = "moving_average(" + value +", 5)"
    else:
        value = "moving_average(sum(" + value +"), 5)"
    keyValue = "moving_average"

query = "SELECT " + value + " FROM \"_\" WHERE (\"__name__\" = \'" + measurement + "\' " + whereAdd + ") AND time >= now() - 3h GROUP BY " + groups # TODO Change "time >= 1529408612543ms AND time <= 1529411212321ms" to "time >= now() - 3h"
#print("Querying using " + query)
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
    #print("Getting data for"),
    for i, tag_key in enumerate(tag_keys):
        tag_string += tag_key + ": " +  tag_only[tag_key]
        value_string += tag_only[tag_key]

        if tag_key not in keys:
            keys.append(tag_key)

        if (i + 1) < len(tag_keys):
            tag_string += " and "
            value_string += " and "

    #print(tag_string)

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

if graphic:
    for value in values:
        plt.plot(value[0], value[1], label=value_string)
    key_string = " and ".join(keys)
    plt.title("metric")
    plt.legend(loc='best', title=key_string)
    plt.show()
else:
    if array or pickle:
        dumper = []
        for i,_ in enumerate(values[0][0]):
            tmp = []
            for value in values:
                tmp.append(value[1][i])
            dumper.append(tmp)

        if array:
            print dumper
        else:
            dump(dumper, file=sys.stdout)
    else:
        for value in values:
            print("\ttime\t\t\tvalue")
            print("---------------------------------------------")
            for i, date in enumerate(value[0]):
                print (str(date) + "\t\t" + str(value[1][i]))
            print("")
