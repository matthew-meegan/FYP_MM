import pandas as pd

anomalies1 = pd.read_parquet("numeric_columns_hourly_anomalies_SESSIONS.parquet")
anomalies2 = pd.read_parquet("numeric_columns_hourly_anomalies_SUM_MB.parquet")

print("Anomalies from numeric_columns_hourly_anomalies.parquet (first 5 rows):")
print(anomalies1.head())
print("\nAnomalies from numeric_columns_hourly_anomalies_SUM_MB.parquet (first 5 rows):")
print(anomalies2.head())


anomaly_indices1 = set(anomalies1['entry'])
anomaly_indices2 = set(anomalies2['entry'])

print(f"\nTotal anomalies in numeric_columns_hourly_anomalies.parquet: {len(anomaly_indices1)}")
print(f"Total anomalies in numeric_columns_hourly_anomalies_SUM_MB.parquet: {len(anomaly_indices2)}")

overlap_indices = anomaly_indices1.intersection(anomaly_indices2)
print(f"\nNumber of overlapping anomalies: {len(overlap_indices)}")
print("Overlapping row indices:", sorted(overlap_indices))
