#!/usr/bin/python

import sys, getopt
import pyKairosDB
import matplotlib.pyplot as plt
import pickle
import math

def usage():
    print("Usage: " + sys.argv[0] + " -i <input file>")

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

if not (infile and metric):
    print >> sys.stderr, ("Not enough arguments not specified")
    usage()
    exit(1)

with open(infile, "rb") as file:
    content = pickle.load(file)

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
    plt.title('scrape_duration_seconds')
    plt.show()
else:
    print data