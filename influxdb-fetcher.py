import datetime
import getopt
import sys

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import requests
from influxdb import InfluxDBClient


def usage():
    print("Usage: python " + sys.argv[0] + " -m <measurement> -g <group>")
    print("    -m --measurement: (required)")
    print("    -g --group: comma separated(required)")
    print("    -v --value: ")
    print("    --rate-time: ")
    print("    -r --rate: ")
    print("    -t --text: ")
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


keyValue = "f64" # TODO fix to use value

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

for tag in result_tags:
    tag_only = tag[1]
    tag_key = list(tag_only.keys())[0]
    print("Getting data for " + str(tag_key) + " " +  str(tag_only[tag_key]))
    get = list(result.get_points(tags=tag_only))

    x = []
    y = []
    for value in get:
        time = datetime.datetime.fromtimestamp(value['time'])
        x.append(time)
        y.append(value[keyValue])
    if graphic:
        plt.plot(x, y, label=tag_only[tag_key])
    else:
        print("\ttime\t\t\tvalue")
        print("---------------------------------------------")
        for i, date in enumerate(x):
            print (str(date) + "\t\t" + str(y[i]))
        print("")


if graphic:
    plt.title("metric")
    plt.legend(loc='best')
    plt.show()
