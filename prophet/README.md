# Prophet 
## Usage
To detect anomalies on a time series run the time series on the function `calculate_anomalies` to get graphs of annomalies, if you want more accurat anomalies, take the time series and filter out obvious anomalies and send it as `filtered_data` to the same function
The timeseries must be a pandas DataFrame with index and column 'ds' as the timestamp of the data and 'y' as the data
