#!/usr/bin/python

import sys, getopt
import pyKairosDB
import requests
import pickle
import re

def usage():
    print("Usage: python kube-learn [-i <ip>] [-p <port>] [-o <output file>]")
    print("    -i --ip: The ip to kairosDB. default: 127.0.0.1")
    print("    -p --port: The port to kairosDB. default: 8080")
    print("    -o --output: File to save serialized result (pickle is used). default: stdout")
    print("    -r --regex: Filter output")
    print("    -b --brief: Shows only the metric names in the database")
    print("    -h --help: Displays this text")

try:
    opts, args = getopt.getopt(sys.argv[1:], 'hbo:i:p:r:', ["help", "output=", "port=", "ip=", "brief", "regex="])
except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(1)

ip = "127.0.0.1"
port = "8080"
outfile = None
brief = False
regex = ""
for o, a in opts:
    if o in ('-i', '--ip'):
        ip = a
    elif o in ('-p', '--port'):
        port = a
    elif o in ('-o', '--output'):
        outfile = a
    elif o in ("-h", "--help"):
        usage()
        exit()
    elif o in ("-b", "--brief"):
        brief = True
    elif o in ("-r", "--regex"):
        regex = a

filter = re.compile(regex)

try:
    con = pyKairosDB.connect(ip, port, False)
except requests.exceptions.ConnectionError as err:
    print >> sys.stderr, (err)
    print()
    usage()
    exit(1)

metric_names = pyKairosDB.metadata.get_all_metric_names(con)

filtered_metrics = []

for name in metric_names:
    if filter.search(name):
        filtered_metrics.append(name)
        if brief:
            print name

if brief:
    exit()

content = con.read_relative(filtered_metrics, (1, 'days'))

if outfile != None:
    with open(outfile, "wb") as file:
        pickle.dump(content, file, pickle.HIGHEST_PROTOCOL)
    print("Wrote results to \"" + outfile + "\"")
else:
    pickle.dump(content, sys.stdout, pickle.HIGHEST_PROTOCOL)
