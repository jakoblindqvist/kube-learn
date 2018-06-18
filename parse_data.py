#!/usr/bin/python

import sys, getopt
import pyKairosDB
import matplotlib.pyplot as plt
import pickle

def usage():
    print("Usage: python " + sys.argv[0] + " -m <metric to fetch> [-i <input file>]")
    print("    -m --metric: The metric to fetch data from")
    print("    -i --infile: The inputfile to read data from. default: stdin")
    print("    -g --graph: Plot the result with matplotlib")
    print("    -h --help: Displays this text")

if len(sys.argv) < 3:
    print >> sys.stderr, ("Too few arguments")
    usage()
    exit(1)

try:
    opts, args = getopt.getopt(sys.argv[1:], 'hgi:m:', ["help", "graph", "infile=", "metric="])
except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(1)

infile = ""
metric = ""
graph = False
for o, a in opts:
    if o in ('-i', '--infile'):
        infile = a
    elif o in ('-m', '--metric'):
        metric = a
    elif o in ('-g', '--graph'):
        graph = True
    elif o in ('-h', '--help'):
        usage()
        exit()

if not metric:
    print >> sys.stderr, ("Not enough arguments not specified")
    usage()
    exit(1)

if infile:
    with open(infile, "rb") as file:
        content = pickle.load(file)
else:
    content = pickle.load(sys.stdin)

result = pyKairosDB.util.get_content_values_by_name(content, metric)

if len(result) < 1:
    print("Can't find any data for metric " + metric)

data = result[0]['values']

if graph:
    x = []
    y = []

    for point in data:
        x.append(point[0])
        y.append(point[1])

    plt.plot(x, y)
    plt.title(metric)
    plt.show()
else:
    print data
    print("")


max = 0
min = sys.maxint
for point in data:
    tmp = int(point[0] * 1000)
    if tmp > max:
        max = tmp
    if tmp < min:
        min = tmp

time = max - min

second, millisecond = divmod(time, 1000)
minute, second = divmod(second, 60)
hour, minute = divmod(minute, 60)
day, hour = divmod(hour, 24)

print("Read " + str(len(data)) + " datapoints for metric \"" + metric + "\"")
print("In a period of %d days %d hours %d minutes %d seconds %d millisecond" % (day, hour, minute, second, millisecond))