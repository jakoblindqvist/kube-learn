import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from fbprophet import Prophet
from workalendar.europe import Sweden
from datetime import timedelta
import time
import math

def weekend_hour(ds, h):
    """Indicator function for weekend hour h."""
    date = pd.to_datetime(ds)
    if date.hour == h and (date.weekday() == 5 or date.weekday() == 6):
        return 1
    else:
        return 0

def get_non_workdays(start, end):
    """Constructs a dataframe with non workdays in the column ds."""
    cal = Sweden()
    datelist = pd.date_range(start, freq='D', end=end)
    days = pd.DataFrame(index=datelist, data=datelist.to_pydatetime(), columns=["ds"])
    non_workdays = days.loc[datelist.map(lambda x: not cal.is_working_day(x))]

    return non_workdays

def generate_holidays(df, periods, freq):
    """Generate non-workdays for all dates in df.index + periods * freq after."""
    start = df.index[0].date()
    future = pd.period_range(df.index[-1].date(), periods=periods, freq=freq)
    end = future.to_timestamp()[-1].date()
    non_workdays = get_non_workdays(start, end)

    holidays = pd.DataFrame({
        'holiday': 'non-workday',
        'ds': non_workdays["ds"],
        'lower_window': 0,
        'upper_window': 0,
    })

    return holidays

def build_model(df, holidays):
    """Initialize the Prophet model with data about holidays and extra regressors."""

    m = Prophet(holidays=holidays, yearly_seasonality=False)

    # Create regressors for all weekend hours
    hours = range(0, 24)
    for h in hours:
        df["weekend_hour{}".format(h)] = df["ds"].apply(lambda x: weekend_hour(x, h))
        m.add_regressor("weekend_hour{}".format(h))

    return m, df

def build_future(m, periods, freq):
    """Create data frame for <periods> * <freq> into the future"""
    future = m.make_future_dataframe(periods=periods, freq=freq, include_history=True)
    # All additional regressors must also be added to the future points.
    for h in range(0, 24):
        future["weekend_hour{}".format(h)] = future['ds'].apply(lambda x: weekend_hour(x, h))

    return future

def get_prediction(m, future):
    """Get a prediction for the specified future."""
    forecast = m.predict(future)
    return forecast.set_index("ds")

def get_residual(data, forecast):
    """Calculate residual of data when removing everything explained by the model."""
    return (data["y"] - forecast["yhat"])

def plot_trend_residual(x_points, y_points_lower, y_points_upper, color='b', alpha=1):
    x = x_points[:]
    x_points.reverse()
    x += x_points

    y_points_upper.reverse()
    y = y_points_lower + y_points_upper
    plt.fill(x, y, color=color, alpha=alpha)

def get_std_dev(data, delta=timedelta(hours=6)):
    # Get variable confidence interval
    current_time = data["ds"][0]
    std_devs = pd.DataFrame()
    times = []
    current_split = pd.DataFrame()

    raw_data = np.array([])

    for i, time in enumerate(data["ds"]):
        raw_data = np.append(raw_data, data["y"][i])

        times.append(time)
        if time - current_time >= delta:
            std_dev_df = pd.DataFrame(data=[np.std(raw_data)] * len(times),
                                      index=times,
                                      columns=['y'])
            std_dev_df["ds"] = times
            std_devs = std_devs.append(std_dev_df)

            times = []
            raw_data = np.array([])
            current_time = time
    std_dev_df = pd.DataFrame(data=[np.std(raw_data)] * len(times),
                              index=times,
                              columns=['y'])
    std_dev_df["ds"] = times
    return std_devs.append(std_dev_df)

def smooth_std_dev(std_dev, smooth_window = timedelta(hours=3)):
    std_dev_list = std_dev["y"].tolist()
    mean_std_dev = [0] * len(std_dev_list)

    start = std_dev.index[0]
    stop = start + smooth_window
    smooth_count = len(std_dev.loc[(std_dev.index >= start) & (std_dev.index <= stop)])

    for i in range(len(std_dev_list)):
        sum = 0
        count = 0
        length = len(std_dev_list)

        for j in range(smooth_count / 2):
            if j < i:
                sum += std_dev_list[i - (j + 1)]
                count += 1

            if i < length - j:
                sum += std_dev_list[i + j]
                count += 1

        mean_std_dev[i] = sum / float(count)

    new_std_dev = std_dev.copy()
    new_std_dev["y"] = pd.Series(mean_std_dev).values
    return new_std_dev

def get_anomalies(dev_forecast, residual, dev_multiplier=1.5, window_delta=timedelta(hours = 4), percent_true=1):
    anomalies = set()

    start = dev_forecast.index[0]
    stop = start + window_delta
    window_size = len(dev_forecast.loc[(dev_forecast.index >= start) & (dev_forecast.index <= stop)])

    num_true = math.ceil(window_size * percent_true)
    window = [False] * window_size
    extreme_window = [False] * window_size
    for i in range(len(residual.index) - window_size + 1):
        new_anomalies = set()
        new_extreme_anomalies = set()
        for j in range(window_size):
            anom_time = residual.index[i + j]
            max_val = dev_forecast.at[anom_time, "yhat"]
            val = residual[anom_time]
            is_anomaly = abs(val) > max_val*dev_multiplier
            is_extreme_anomaly = abs(val) > max_val*dev_multiplier*4
            extreme_window[j] = is_extreme_anomaly
            window[j] = is_anomaly
            if is_anomaly:
                new_anomalies.add((anom_time, val))
            if is_extreme_anomaly:
                new_extreme_anomalies.add((anom_time, val))

        if window.count(True) >= num_true:
            anomalies |= new_anomalies

        if extreme_window.count(True) > 0:
            anomalies |= new_extreme_anomalies

    return sorted(list(anomalies))

def plot_anom(data, forecast, forecast_std_dev, dev_multiplier=1.5, figsize=(20,10), window_delta=timedelta(hours = 4), percent_true=1):
    plt.figure(figsize=figsize)
    residual = get_residual(data, forecast)

    upper = dev_multiplier*forecast_std_dev["yhat"]
    lower = -dev_multiplier*forecast_std_dev["yhat"]

    anomalies = get_anomalies(forecast_std_dev, residual, window_delta = window_delta, percent_true = percent_true)
    concurrent = []
    data_time_delta = (data.index[1] - data.index[0]) * 2
    for (anom_time, value) in anomalies:
        if len(concurrent) == 0 or (anom_time - concurrent[-1]) <= data_time_delta:
            concurrent.append(anom_time)
        else:
            print "Anomalies between", concurrent[0], " - ", concurrent[-1]
            concurrent = []
        plt.plot(anom_time, value, 'ro')
    if len(concurrent) > 0:
        print "Anomalies between", concurrent[0], " - ", concurrent[-1]

    plot_trend_residual(forecast_std_dev.index.tolist(), lower.tolist(), upper.tolist(), color='b', alpha=0.2)
    plt.plot(residual)
    plt.show()

    plt.figure(figsize=figsize)
    residual = get_residual(data, forecast)

    upper = dev_multiplier*forecast_std_dev["yhat"] + forecast["yhat"]
    lower = -dev_multiplier*forecast_std_dev["yhat"] + forecast["yhat"]

    for (anom_time, value) in anomalies:
        plt.plot(anom_time, value + forecast.at[anom_time, "yhat"], 'ro')

    plot_trend_residual(forecast_std_dev.index.tolist(), lower.tolist(), upper.tolist(), color='b', alpha=0.2)

    plt.plot(data["y"], 'b')
    plt.plot(forecast["yhat"], 'orange')
    plt.show()

def calculate_anomalies(raw_data, filtered_data=[], window_delta=timedelta(hours = 4), percent_true=1, std_dev_smoothing="1H"):
    if len(filtered_data) == 0:
        print "No filtered data, using raw data"
        filtered_data = raw_data

    periods = 1
    freq = 'H'

    holidays = generate_holidays(raw_data, periods, freq)
    print "Building data model...",
    start = time.time()
    model, new_filtered = build_model(filtered_data, holidays)
    # Train/fit the model
    model.fit(new_filtered)
    print " Done in", time.time() - start, "s"

    future = build_future(model, periods, freq)

    print "Forecasting data...",
    start = time.time()
    forecast = get_prediction(model, future)
    #forecast = model.predict(future)
    #fig = model.plot(forecast)
    #fig.set_size_inches(22, 9)
    #model.plot_components(forecast)
    #forecast = forecast.set_index("ds")
    print " Done in", time.time() - start, "s"

    print "Getting standard deviation...",
    start = time.time()
    std_devs = get_std_dev(filtered_data, delta=timedelta(hours=2))
    print " Done in", time.time() - start, "s"

    std_devs = std_devs.resample(std_dev_smoothing, label='right').mean()
    std_devs["ds"] = std_devs.index

    print "Building standard deviation model...",
    start = time.time()
    model, new_filtered = build_model(std_devs, holidays)
    # Train/fit the model
    model.fit(new_filtered)
    print " Done in", time.time() - start, "s"

    future = build_future(model, periods, freq)

    print "Forecasting standard deviation...",
    start = time.time()
    forecast_std_dev = get_prediction(model, future)
    #forecast_std_dev = model.predict(future)
    #fig = model.plot(forecast_std_dev)
    #fig.set_size_inches(22, 9)
    #model.plot_components(forecast_std_dev)
    #forecast_std_dev = forecast_std_dev.set_index("ds")
    print " Done in", time.time() - start, "s"

    # fill empty
    print "Filling points...",
    start = time.time()
    current_value = forecast_std_dev["yhat"].tolist()[0]
    values = []
    times = []
    for forecast_std_dev_time in forecast_std_dev.index:
        if forecast_std_dev_time not in forecast.index:
            forecast_std_dev = forecast_std_dev.drop(forecast_std_dev_time)

    for forcast_time in forecast.index:
        if forcast_time in forecast_std_dev.index:
            current_value = forecast_std_dev.at[forcast_time, "yhat"]
        else:
            values.append(current_value)
            times.append(forcast_time)
    forecast_std_dev = forecast_std_dev.append(pd.DataFrame(data=values,
                                                            index=times,
                                                            columns=["yhat"]))

    forecast_std_dev["ds"] = forecast_std_dev.index
    forecast_std_dev = forecast_std_dev.sort_values('ds')
    print " Done in", time.time() - start, "s"

    plot_anom(raw_data, forecast, forecast_std_dev, window_delta=window_delta, percent_true=percent_true)

def calculate_anomalies_multiple(raw_datas, filtered_datas=[], window_delta=timedelta(hours = 4), percent_true=1, std_dev_smoothing="1H"):
    """ TODO """
    if len(filtered_datas) == 0:
        filtered_datas = raw_datas

    raw_datas_len = len(raw_datas)
    filtered_dats_len = len(filtered_datas)

    if raw_datas_len != filtered_dats_len:
        print "Error: Length of raw_datas and filtered_dats are different!!"
        return

    for i in range(raw_datas_len):
        print "Calculating data", i + 1, "of", raw_datas_len, "..."
        start = time.time()
        calculate_anomalies(raw_datas[i], filtered_datas[i], window_delta=window_delta, percent_true=percent_true, std_dev_smoothing=std_dev_smoothing)
        elapsed = time.time()
        elapsed = elapsed - start
        print "Done in", elapsed, "s"

if __name__ == '__main__':
    print "Prophet anomaly detector loaded"
