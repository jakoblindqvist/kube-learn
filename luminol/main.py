#!/usr/bin/python
"""luminol"""
from __future__ import print_function
import json
import matplotlib.pyplot as plt
import pandas as pd
from luminol import anomaly_detector as ad
import influx_fetcher


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
    # data['y'] = np.log(data['y'])
    # data = data.resample('10S').mean()
    # for feature_name in data:
    #     data[feature_name] = np.log(data[feature_name])
    data = data.dropna()
    # data['ds'] = data.index
    return data


def find_anomalies(data):
    """Run luminol and find anomalies"""
    points = {}
    i = 0
    for point in data:
        points[i] = point
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
    """Converts influxdb metrics to a pandas dataframe"""
    # res = {}
    # for feature in data['feature_names']:
    #     res[feature] = []

    # for series in data['data']:
    #     for pair in zip(data['feature_names'], series):
    #         res[pair[0]].append(pair[1])

    # data = pd.DataFrame(data=res, index=data['times'])

    data = pd.DataFrame(data=data['data'],
                        index=data['times'],
                        columns=data['feature_names'])
    return data

def pca_reduce(data):
    pass


def main():
    """Read data from csv-file and plot anomalies using luminol"""

    # Get metrics from influx
    conf = influx_fetcher.InfluxConfig(ip='212.32.186.84')
    with open('metrics.json') as file_handle:
        query = json.load(file_handle)
    data = influx_fetcher.get_metrics(query, conf)

    # Prepare data
    times = data['times']
    feature_names = data['feature_names']
    data = influx_to_dataframe(data)
    data = prepare_data(data)

    # Process data using PCA
    data = pca_reduce(data)
    
    print(data.head())

    # Find anomalies
    anomalies = []
    for feature_name in data:
        series = data[feature_name]
        anomalies.append(find_anomalies(series))

    anomaly_coordinates = []
    for anomaly in anomalies:
        anomaly_coordinates.append(get_anomaly_index(anomaly))
    anomaly_coordinates = zip(feature_names, anomaly_coordinates)

    # Plot the metrics
    data.plot()

    # Plot the anomalies
    print(anomaly_coordinates)
    for anomaly in anomaly_coordinates:
        feature_name = anomaly[0]
        indexes = anomaly[1]
        xcoords = []
        ycoords = []
        for index in indexes:
            xcoords.append(times[index])
            ycoords.append(data[feature_name][index])
        plt.scatter(xcoords, ycoords, c='r')

    # Display the plot with metrics and anomalies
    plt.show()


if __name__ == '__main__':
    main()
