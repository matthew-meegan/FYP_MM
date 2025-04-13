import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
from statsmodels.tsa.arima.model import ARIMA
import warnings
import os

warnings.filterwarnings("ignore")

def detect_anomalies(ts_log, initial_train=120, forecast_horizon=48):
  
    start = time.time()
    n = len(ts_log)
    training_end = initial_train
    anomalies = []
    forecast_results = pd.DataFrame(columns=["datetime", "forecast", "lower", "upper", "actual"])
    
    entry_mapping = {dt: idx + 1 for idx, dt in enumerate(ts_log.index)}
    
    while training_end + forecast_horizon <= n:
        train_data = ts_log.iloc[:training_end]
        test_data = ts_log.iloc[training_end: training_end + forecast_horizon]
        print(f"Expanding window: Training size = {len(train_data)}, Test size = {len(test_data)}")
        
        try:
            model = ARIMA(train_data, order=(1, 0, 0), seasonal_order=(1, 0, 2, 24))
            model_fit = model.fit()
        except Exception as e:
            print(f"Error fitting ARIMA model with training size {len(train_data)}: {e}")
            break
        
        try:
            forecast_obj = model_fit.get_forecast(steps=forecast_horizon)
            forecast_mean = np.array(forecast_obj.predicted_mean).flatten()
            conf_int = np.array(forecast_obj.conf_int(alpha=0.0027))  # 99.73% CI (~3 sigma)
        except Exception as e:
            print(f"Error during forecasting with training size {len(train_data)}: {e}")
            training_end += forecast_horizon
            continue
        
        test_index = test_data.index
        for i, dt in enumerate(test_index):
            actual_log = test_data.iloc[i]
            forecast_log = forecast_mean[i]
            lower_log = conf_int[i, 0]
            upper_log = conf_int[i, 1]
            # Back-transform from log scale
            actual_val = np.expm1(actual_log)
            forecast_val = np.expm1(forecast_log)
            lower_bound = np.expm1(lower_log)
            upper_bound = np.expm1(upper_log)
            
            # Determine the entry number for this timestamp
            entry_num = entry_mapping.get(dt, None)
            
            if actual_val < lower_bound or actual_val > upper_bound:
                anomalies.append({
                    "entry": entry_num,
                    "datetime": dt,
                    "actual": actual_val,
                    "forecast": forecast_val,
                    "lower_bound": lower_bound,
                    "upper_bound": upper_bound
                })
            
            forecast_results = forecast_results.append({
                "datetime": dt,
                "forecast": forecast_val,
                "lower": lower_bound,
                "upper": upper_bound,
                "actual": actual_val
            }, ignore_index=True)
        
        training_end += forecast_horizon
    
    runtime = time.time() - start
    return anomalies, forecast_results, runtime

def process_file(file_name, sample_fraction):
    
    print(f"\n=== Processing file: {file_name} ({sample_fraction}% sample) ===")
    df = pd.read_parquet(file_name)
    df['datetime'] = pd.to_datetime(df['USAGE_DATE'], errors='coerce') + pd.to_timedelta(df['SESSION_HOUR'], unit='h')
    df = df.sort_values("datetime").set_index("datetime")
    
    ts_raw = df["SUM_MB"].copy()
    ts_raw = pd.to_numeric(ts_raw, errors='coerce').dropna()
    
    # Log transform for robust modeling
    ts_log = np.log1p(ts_raw)
    ts_log.name = "LOG_SUM_MB"
    
    anomalies, forecast_results, runtime = detect_anomalies(ts_log)
    print(f"File {file_name}: Detected {len(anomalies)} anomalies in {runtime:.2f} sec.")
    
    # List each detected anomaly with its entry number
    if anomalies:
        print("\nAnomaly details:")
        for anomaly in anomalies:
            print(f"Entry: {anomaly['entry']}, Datetime: {anomaly['datetime']}, Actual: {anomaly['actual']:.2f}, "
                  f"Forecast: {anomaly['forecast']:.2f}, Lower Bound: {anomaly['lower_bound']:.2f}, "
                  f"Upper Bound: {anomaly['upper_bound']:.2f}")
    
    # Save forecast results to a parquet file
    base_name = os.path.splitext(file_name)[0]
    forecast_file = f"{base_name}_forecast_results.parquet"
    forecast_results.to_parquet(forecast_file)
    print(f"Saved forecast results to {forecast_file}")
    
    # Save anomalies as a parquet file if any anomalies exist
    if anomalies:
        anomalies_df = pd.DataFrame(anomalies)
        anomalies_file = f"{base_name}_anomalies_SESSIONS.parquet"
        anomalies_df.to_parquet(anomalies_file)
        print(f"Saved anomalies to {anomalies_file}")
    
    # Plot the results on original scale
    plt.figure(figsize=(14, 7))
    plt.plot(ts_raw.index, ts_raw, label="Actual SUM_MB", color="blue", alpha=0.6)
    if not forecast_results.empty:
        forecast_results['datetime'] = pd.to_datetime(forecast_results['datetime'])
        plt.fill_between(forecast_results['datetime'],
                         forecast_results['lower'],
                         forecast_results['upper'],
                         color="gray", alpha=0.3, label="99.73% Forecast CI")
        plt.plot(forecast_results['datetime'], forecast_results['forecast'], color="orange", linestyle="--", label="Forecast Mean")
    if anomalies:
        anomaly_times = [a["datetime"] for a in anomalies]
        anomaly_values = [a["actual"] for a in anomalies]
        plt.scatter(anomaly_times, anomaly_values, color="red", label="Anomalies", zorder=5)
    plt.xlabel("Datetime", fontsize=22)
    plt.ylabel("SUM_MB", fontsize=22)
    plt.title(f"ARIMA(1,0,0)(1,0,2)[24] Anomaly Detection ({sample_fraction}% Sample) - Original Scale", fontsize=26)
    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)
    plt.legend(fontsize=20)
    plt.tight_layout()
    plt.show()
    
    # Plot the results on log-transformed scale
    plt.figure(figsize=(14, 7))
    plt.plot(ts_log.index, ts_log, label="Log(1+SUM_MB)", color="blue", alpha=0.6)
    if not forecast_results.empty:
        # Calculate log-transformed forecast results
        forecast_results['forecast_log'] = np.log1p(forecast_results['forecast'])
        forecast_results['lower_log'] = np.log1p(forecast_results['lower'])
        forecast_results['upper_log'] = np.log1p(forecast_results['upper'])
        plt.fill_between(forecast_results['datetime'],
                         forecast_results['lower_log'],
                         forecast_results['upper_log'],
                         color="gray", alpha=0.3, label="99.73% Forecast CI (log scale)")
        plt.plot(forecast_results['datetime'], forecast_results['forecast_log'], color="orange", linestyle="--", label="Forecast Mean (log scale)")
    if anomalies:
        anomaly_times = [a["datetime"] for a in anomalies]
        anomaly_log_values = [np.log1p(a["actual"]) for a in anomalies]
        plt.scatter(anomaly_times, anomaly_log_values, color="red", label="Anomalies (log scale)", zorder=5)
    plt.xlabel("Datetime", fontsize=22)
    plt.ylabel("log(1+SUM_MB)", fontsize=22)
    plt.title(f"ARIMA(1,0,0)(1,0,2)[24] Anomaly Detection ({sample_fraction}% Sample) - Log Scale", fontsize=26)
    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)
    plt.legend(fontsize=20)
    plt.tight_layout()
    plt.show()
    
    return len(anomalies), runtime

def main():
    overall_start = time.time()
    
    # Define file names for each sample fraction, including full aggregated data (100%)
    sample_files = [
        ("numeric_columns_hourly_1.parquet", 1),
        #("numeric_columns_hourly_5.parquet", 5),
        #("numeric_columns_hourly_15.parquet", 15),
        #("numeric_columns_hourly_30.parquet", 30),
        #("numeric_columns_hourly_50.parquet", 50),
        #("numeric_columns_hourly_80.parquet", 80),
        ("numeric_columns_hourly.parquet", 100)
    ]
    
    summary = []
    for file_name, perc in sample_files:
        anomaly_count, runtime = process_file(file_name, perc)
        summary.append((perc, anomaly_count, runtime))
    
    overall_end = time.time()
    total_overall = overall_end - overall_start
    
    print("\n=== Summary of Anomaly Detection Results ===")
    for perc, count, rt in summary:
        print(f"{perc}% sample: {count} anomalies detected, runtime: {rt:.2f} sec")
    print(f"Total execution time (including plotting for all samples): {total_overall:.2f} sec")

if __name__ == "__main__":
    main()
