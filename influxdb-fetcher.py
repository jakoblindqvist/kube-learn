import datetime
import getopt
import sys

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import requests
from influxdb import InfluxDBClient


def usage():
    print("Usage: python " + sys.argv[0] + " -m <measurement> -g <group>")
    print("    -m --measurement: The measurement to fetch (required)")
    print("    -g --group: Which tags to group by separaded by comma (required)")
    print("    -v --value: The values to fetch. default: f64")
    print("    -r --rate: Specify if the data should be \"ratified\"")
    print("       --rate-time: If rate is specified, change how long the rate is. default: 1s")
    print("    -t --text: Prints the output to stdout instead of a plot")
    print("    -h --help: Displays this text")

try:
    opts, args = getopt.getopt(sys.argv[1:], 'hrtm:g:v:', ["help", "rate", "text", "measurement=", "group=", "value=", "rate-time="])
except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(1)

measurement = ""
groups = ""
value = "f64"
rateTime = "1s"
rate = False
graphic = True
for o, a in opts:
    if o in ('-m', '--measurement'):
        measurement = a
    elif o in ('-g', '--group'):
        groups = a
    elif o in ('-v', '--value'):
        value = a
    elif o == 'rate-time':
        rateTime = a
    elif o in ('-t', '--text'):
        graphic = False
    elif o in ('-r', '--rate'):
        rate = True
    elif o in ("-h", "--help"):
        usage()
        exit()

if not (measurement and groups):
    print >> sys.stderr, ("ERROR: measurement and group is mandatory parameters")
    usage()
    exit(1)

keyValue = value.split(',')[0] # TODO fix to use all values


# Create group string
if rate:
    groups = "time(" + rateTime + "), " + groups
    value = "derivative(sum(" + value +"), " + rateTime + ")"
    keyValue = "derivative"

query = "SELECT " + value + " FROM \"_\" WHERE (\"__name__\" = \'" + measurement + "\') AND time >= 1529408612543ms AND time <= 1529411212321ms GROUP BY " + groups

try:
    client = InfluxDBClient('localhost', 8086, 'prom', 'prom', 'prometheus')
    result = client.query(query, epoch='s')
except requests.exceptions.ConnectionError as error:
    print >> sys.stderr, ("Error connecting to database")
    exit(1)

result_tags = list(result.keys())

keys = []

for tag in result_tags:
    tag_only = tag[1]
    tag_keys = list(tag_only.keys())
    value_string = ""
    tag_string = ""
    print("Getting data for"),
    for i, tag_key in enumerate(tag_keys):
        tag_string += tag_key + ": " +  tag_only[tag_key]
        value_string += tag_only[tag_key]

        if tag_key not in keys:
            keys.append(tag_key)

        if (i + 1) < len(tag_keys):
            tag_string += " and "
            value_string += " and "

    print(tag_string)

    get = list(result.get_points(tags=tag_only))

    x = []
    y = []
    for value in get:
        time = datetime.datetime.fromtimestamp(value['time'])
        x.append(time)
        y.append(value[keyValue])

    if graphic:
        plt.plot(x, y, label=value_string)
    else:
        print("\ttime\t\t\tvalue")
        print("---------------------------------------------")
        for i, date in enumerate(x):
            print (str(date) + "\t\t" + str(y[i]))
        print("")

if graphic:
    key_string = " and ".join(keys)
    plt.title("metric")
    plt.legend(loc='best', title=key_string)
    plt.show()
