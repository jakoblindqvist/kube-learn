#!/usr/bin/python
"""luminol"""
from __future__ import print_function
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys
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
    data['y'] = np.log(data['y'])
    data = data.resample('10T').mean()
    data = data.dropna()
    data['ds'] = data.index
    return data


def find_anomalies(data):
    """Run luminol and find anomalies"""
    points = {}
    i = 0
    for _, row, _ in data.itertuples():
        points[i] = row
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


def main():
    """Read data from csv-file and plot anomalies using luminol"""

    # read csv path from cli
    if(len(sys.argv) != 2):
        print("usage: python main.py <path-to-csv>", file=sys.stderr)
        sys.exit(1)
    csv_path = sys.argv[1]

    # read data from csv
    data = read_csv(csv_path)
    data = prepare_data(data)

    # find anomalies
    anomalies = find_anomalies(data)
    points = get_anomaly_index(anomalies)

    # convert luminol's anomalies into coordinates
    xcoords = []
    ycoords = []
    for point in points:
        ycoords.append(data['y'][point])
        xcoords.append(data['ds'][point])

    # plot result
    data.plot(x='ds', y='y')
    plt.scatter(xcoords, ycoords, c='r')
    plt.show()


if __name__ == '__main__':
    main()
