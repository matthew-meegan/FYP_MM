import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import adfuller, kpss
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
import warnings

warnings.filterwarnings('ignore')

file_name = 'numeric_columns_hourly.parquet'
df = pd.read_parquet(file_name)

df['datetime'] = pd.to_datetime(df['USAGE_DATE'], errors='coerce') + pd.to_timedelta(df['SESSION_HOUR'], unit='h')
df = df.sort_values("datetime").set_index("datetime")

print("First few rows of the time series data:")
print(df.head())

df['SUM_MB'] = df['SUM_MB'].astype(float)

plt.figure(figsize=(12, 6))
plt.plot(df.index, df['SUM_MB'], label='Original SUM_MB', color='blue')
plt.title('Original Time Series', fontsize=18)
plt.xlabel('Datetime', fontsize=16)
plt.ylabel('SUM_MB', fontsize=16)
plt.legend(fontsize=14)
plt.tight_layout()
plt.show()

def adf_test(series, title=''):
   
    print(f'\nAugmented Dickey-Fuller Test: {title}')
    result = adfuller(series.dropna(), autolag='AIC')
    labels = ['ADF Test Statistic', 'p-value', '#Lags Used', 'Number of Observations Used']
    for value, label in zip(result[:4], labels):
        print(f'{label} : {value:.4f}')
    for key, val in result[4].items():
        print(f'Critical Value ({key}) : {val:.4f}')
    if result[1] <= 0.05:
        print("=> Reject the null hypothesis. Data is likely stationary.")
    else:
        print("=> Failed to reject the null hypothesis. Data is likely non-stationary.")

def kpss_test(series, **kw):
    
    print("\nKPSS Test:")
    statistic, p_value, n_lags, critical_values = kpss(series.dropna(), **kw)
    print(f'KPSS Statistic: {statistic:.4f}')
    print(f'p-value: {p_value:.4f}')
    print(f'Number of Lags: {n_lags}')
    for key, value in critical_values.items():
        print(f'Critical Value ({key}): {value:.4f}')
    if p_value < 0.05:
        print("=> The series is likely non-stationary (reject stationarity).")
    else:
        print("=> The series is likely stationary (fail to reject stationarity).")

# Run tests on the original series (using the SUM_MB column)
adf_test(df['SUM_MB'], title='Original SUM_MB Series')
kpss_test(df['SUM_MB'], regression='c')

df['log_SUM_MB'] = np.log1p(df['SUM_MB'])

plt.figure(figsize=(12, 6))
plt.plot(df.index, df['log_SUM_MB'], label='Log-Transformed SUM_MB', color='green')
plt.title('Log-Transformed Time Series', fontsize=18)
plt.xlabel('Datetime', fontsize=16)
plt.ylabel('log(SUM_MB)', fontsize=16)
plt.legend(fontsize=14)
plt.tight_layout()
plt.show()

# Run stationarity tests on the log-transformed series
adf_test(df['log_SUM_MB'], title='Log-Transformed SUM_MB Series')
kpss_test(df['log_SUM_MB'], regression='c')

# Compute the first difference of the log-transformed series to remove trend.
df['log_SUM_MB_diff'] = df['log_SUM_MB'].diff()

plt.figure(figsize=(12, 6))
plt.plot(df.index, df['log_SUM_MB_diff'], label='Differenced Log-Transformed Series', color='purple')
plt.title('Differenced Log-Transformed Time Series', fontsize=18)
plt.xlabel('Datetime', fontsize=16)
plt.ylabel('Difference of log(SUM_MB)', fontsize=16)
plt.legend(fontsize=14)
plt.tight_layout()
plt.show()

# Run stationarity tests on the differenced series
adf_test(df['log_SUM_MB_diff'].dropna(), title='Differenced Log-Transformed SUM_MB Series')
kpss_test(df['log_SUM_MB_diff'].dropna(), regression='c')

plt.figure(figsize=(12, 6))
plot_acf(df['log_SUM_MB_diff'].dropna(), lags=40, title='ACF of Differenced Log-Transformed Series')
plt.xticks(fontsize=14)
plt.yticks(fontsize=14)
plt.tight_layout()
plt.show()

plt.figure(figsize=(12, 6))
plot_pacf(df['log_SUM_MB_diff'].dropna(), lags=40, title='PACF of Differenced Log-Transformed Series', method='ywm')
plt.xticks(fontsize=14)
plt.yticks(fontsize=14)
plt.tight_layout()
plt.show()
