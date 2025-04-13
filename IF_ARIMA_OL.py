import pandas as pd
import numpy as np
import re

def extract_row_indices_from_txt(file_path):
    
    row_indices = []
    pattern = re.compile(r"Row Index:\s*(\d+)")
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            match = pattern.search(line)
            if match:
                row_indices.append(int(match.group(1)))
    return set(row_indices)

# Load ARIMA forecast results.
arima_results = pd.read_parquet("numeric_columns_hourly_forecast_results.parquet")
print("ARIMA Forecast Results (first 5 rows):")
print(arima_results.head())

# Create an anomaly flag for ARIMA:
arima_results['anomaly_arima'] = np.where(
    (arima_results['actual'] < arima_results['lower']) | (arima_results['actual'] > arima_results['upper']),
    1, 0
)

arima_anomaly_indices = set(arima_results[arima_results['anomaly_arima'] == 1].index)
print(f"\nTotal anomalies detected in ARIMA parquet: {len(arima_anomaly_indices)}")
print("ARIMA anomaly indices:", sorted(arima_anomaly_indices))

if_anomaly_txt_file = "Anomaly_Details_Full.txt"  # Adjust this name if necessary

if_anomaly_indices = extract_row_indices_from_txt(if_anomaly_txt_file)
print(f"\nTotal anomalies detected in IF txt file: {len(if_anomaly_indices)}")
print("IF anomaly indices:", sorted(if_anomaly_indices))

overlap_indices = arima_anomaly_indices.intersection(if_anomaly_indices)
print(f"\nNumber of overlapping anomalies: {len(overlap_indices)}")
print("Overlapping row indices:", sorted(overlap_indices))
