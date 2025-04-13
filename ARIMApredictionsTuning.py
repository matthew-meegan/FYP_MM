import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pmdarima import auto_arima
import warnings
import time

warnings.filterwarnings("ignore", category=FutureWarning)

def main():
    start_time = time.time()  # Start timing

  
    df = pd.read_parquet("numeric_columns_hourly.parquet")
    df['datetime'] = pd.to_datetime(df['USAGE_DATE'], errors='coerce') + pd.to_timedelta(df['SESSION_HOUR'], unit='h')
    df = df.sort_values("datetime").set_index("datetime")

    # Use "SUM_MB" as the numeric target
    ts_raw = df["SUM_MB"].copy()
    ts_raw = pd.to_numeric(ts_raw, errors='coerce').dropna()

 
    ts = np.log1p(ts_raw)
    ts.name = "LOG_SUM_MB"

    print(f"Total data points in series: {len(ts)}")

  
    try:
        tuned_model = auto_arima(
            ts,
            start_p=0, start_q=0,
            max_p=3, max_q=3, max_d=2,
            seasonal=True, m=24,  # hourly data with daily seasonality
            stepwise=True, trace=True,
            error_action='ignore',
            suppress_warnings=True
        )
        print("Optimal ARIMA order:", tuned_model.order, "seasonal_order:", tuned_model.seasonal_order)
    except Exception as e:
        print("Error in auto_arima:", e)
        return

   
    pred_obj = tuned_model.arima_res_.get_prediction(start=0, end=len(ts)-1)
    pred_mean = pred_obj.predicted_mean   # on log scale
    conf_int = pred_obj.conf_int(alpha=0.0027)  # ~99.73% confidence interval

    forecast_results = pd.DataFrame({
        "datetime": ts.index,
        "forecast_log": pred_mean,
        "lower_log": conf_int[:, 0],
        "upper_log": conf_int[:, 1],
        "actual_log": ts
    })
    forecast_results["forecast"] = np.expm1(forecast_results["forecast_log"])
    forecast_results["lower"] = np.expm1(forecast_results["lower_log"])
    forecast_results["upper"] = np.expm1(forecast_results["upper_log"])
    forecast_results["actual"] = np.expm1(forecast_results["actual_log"])

    # Flag anomalies where the actual value falls outside of the forecast interval.
    anomalies = []
    for i, row in forecast_results.iterrows():
        if row["actual"] < row["lower"] or row["actual"] > row["upper"]:
            anomalies.append({
                "datetime": row["datetime"],
                "actual": row["actual"],
                "forecast": row["forecast"],
                "lower_bound": row["lower"],
                "upper_bound": row["upper"]
            })

    
    print("\nDetected Anomalies:")
    if anomalies:
        for anomaly in anomalies:
            print(f"Time: {anomaly['datetime']}, Actual: {anomaly['actual']:.2f}, Forecast: {anomaly['forecast']:.2f}, "
                  f"CI = ({anomaly['lower_bound']:.2f}, {anomaly['upper_bound']:.2f})")
    else:
        print("No anomalies detected.")

    plt.figure(figsize=(14,6))
    # Plot the raw data
    plt.plot(ts_raw.index, ts_raw, label="Actual SUM_MB", color="blue", alpha=0.5)
    # Plot forecast confidence intervals
    plt.fill_between(forecast_results["datetime"],
                     forecast_results["lower"],
                     forecast_results["upper"],
                     color="gray", alpha=0.3, label="~99.73% Forecast CI")
    plt.plot(forecast_results["datetime"], forecast_results["forecast"], color="orange", linestyle="--", label="Forecast Mean")
    # Mark anomalies
    if anomalies:
        anomaly_times = [a["datetime"] for a in anomalies]
        anomaly_values = [a["actual"] for a in anomalies]
        plt.scatter(anomaly_times, anomaly_values, color="red", label="Anomalies", zorder=5)
    plt.xlabel("Datetime")
    plt.ylabel("SUM_MB")
    plt.title("ARIMA Model (Trained on Entire Dataset) with 3-Sigma Anomaly Detection")
    plt.legend()
    plt.tight_layout()
    plt.show()

    end_time = time.time()
    print(f"\nTotal execution time: {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
