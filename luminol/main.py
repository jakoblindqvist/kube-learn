#!/usr/bin/python
"""luminol"""
from __future__ import print_function
import json
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys
from influx_fetcher import *
from luminol import anomaly_detector as ad


def read_csv(path):
    """Read csv into a data frame"""
    data = pd.read_csv(path,
                       header=None,
                       names=['ds', 'y'],
                       index_col=0,
                       parse_dates=True)
    return data


def prepare_data(data):
    """Prepare the data"""
    #data['y'] = np.log(data['y'])
    # for column in data:
    #     data[column] = np.log(data[column])
    # data = data.resample('10S').mean()
    data = data.dropna()
    # data['ds'] = data.index
    return data


def find_anomalies(data):
    """Run luminol and find anomalies"""
    points = {}
    i = 0
    # for _, row, _ in data.itertuples():
    #     points[i] = row
    #     i += 1
    for row in data.itertuples():
        points[i] = row[1]
        i += 1
    detector = ad.AnomalyDetector(points,
                                  algorithm_name='default_detector')
    anomalies = detector.get_anomalies()
    return anomalies


def get_anomaly_index(anomalies):
    """Return indexes for luminol anomalies"""
    points = []
    # for anomaly in anomalies:
    #     window = anomaly.get_time_window()
    #     for point in range(window[0], window[1]):
    #         points.append(point)
    for anomaly in anomalies:
        points.append(anomaly.exact_timestamp)
    return points


def influx_to_dataframe(data):
    data = pd.DataFrame(data=data['data'],
                        index=data['times'],
                        columns=data['feature_names'])
    return data


def main():
    """Read data from csv-file and plot anomalies using luminol"""

    # Get metrics from influx
    conf = InfluxConfig(ip = '212.32.186.84')
    with open('metrics.json') as file_handle:
        query = json.load(file_handle)
    data = get_metrics(query, conf)

    # Prepare data
    data = influx_to_dataframe(data)
    data = prepare_data(data)

    # Find anomalies
    anomalies = find_anomalies(data)
    points = get_anomaly_index(anomalies)

    # Plot data and anomalies
    data.plot()
    plt.show()
    
    # xcoords = []
    # ycoords = []
    # for point in points:
    #     ycoords.append(data['times'][point])
    #     xcoords.append(data['data']['master'][point])

    # data.plot()
    # plt.show()

    # # read csv path from cli
    # if(len(sys.argv) != 2):
    #     print("usage: python main.py <path-to-csv>", file=sys.stderr)
    #     sys.exit(1)
    # csv_path = sys.argv[1]

    # # read data from csv
    # data = read_csv(csv_path)
    # data = prepare_data(data)

    # # find anomalies
    # anomalies = find_anomalies(data)
    # points = get_anomaly_index(anomalies)

    # # convert luminol's anomalies into coordinates
    # xcoords = []
    # ycoords = []
    # for point in points:
    #     ycoords.append(data['y'][point])
    #     xcoords.append(data['ds'][point])

    # # plot result
    # data.plot(x='ds', y='y')
    # plt.scatter(xcoords, ycoords, c='r')
    # plt.show()


if __name__ == '__main__':
    main()
