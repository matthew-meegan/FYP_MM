import time
from snowflake_connection import conn, cur  # Import persistent connection

# Function to run queries and measure execution time
def run_query(query, description="Query"):
    print(f"\n⏳ Running: {description} ...")
    start_time = time.time()
    cur.execute(query)
    result = cur.fetchall()
    elapsed_time = time.time() - start_time
    print(f"✅ {description} completed in {elapsed_time:.2f} sec")
    return result


columns = [
    "USAGE_DATE",
    "SESSION_HOUR",
    "VEHICLE_ID",
    "COUNTRY",
    "SERVINGNETWORK",
    "OPERATOR",
    "RAT",
    "APN",
    "REGISTRATION_COHORT",
    "ECUTYPE",
    "TOTAL_MB_CHARGED",
    "TOTAL_SESSIONS",
    "IS_NON_ZERO_SESSION"
]


numeric_summary_query = """
SELECT 
    MIN(TOTAL_MB_CHARGED), MAX(TOTAL_MB_CHARGED), AVG(TOTAL_MB_CHARGED), STDDEV(TOTAL_MB_CHARGED),
    MIN(TOTAL_SESSIONS), MAX(TOTAL_SESSIONS), AVG(TOTAL_SESSIONS), STDDEV(TOTAL_SESSIONS)
FROM CTRF_PROD.DATA_QUALITY.SAMPLE_DATA
"""
numeric_summary = run_query(numeric_summary_query, "Numeric Summary Statistics")
(min_mb, max_mb, avg_mb, std_mb, min_sess, max_sess, avg_sess, std_sess) = numeric_summary[0]
print("\n📊 Numeric Summary Statistics:")
print(f"TOTAL_MB_CHARGED - Min: {min_mb}, Max: {max_mb}, Avg: {avg_mb}, Std Dev: {std_mb}")
print(f"TOTAL_SESSIONS   - Min: {min_sess}, Max: {max_sess}, Avg: {avg_sess}, Std Dev: {std_sess}")


cohort_sessions_query = """
SELECT REGISTRATION_COHORT, SUM(TOTAL_SESSIONS) AS total_sessions
FROM CTRF_PROD.DATA_QUALITY.SAMPLE_DATA
GROUP BY REGISTRATION_COHORT
ORDER BY total_sessions DESC
"""
cohort_sessions = run_query(cohort_sessions_query, "Session Activity per Registration Cohort")
print("\n📅 Session Activity per Registration Cohort:")
for row in cohort_sessions:
    print(f"{row[0]}: {row[1]} sessions")

non_zero_sessions_query = """
SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM CTRF_PROD.DATA_QUALITY.SAMPLE_DATA)
FROM CTRF_PROD.DATA_QUALITY.SAMPLE_DATA
WHERE IS_NON_ZERO_SESSION = TRUE
"""
non_zero_sessions = run_query(non_zero_sessions_query, "Non-Zero Session Percentage")
print(f"\n✅ Non-Zero Session Percentage: {non_zero_sessions[0][0]:.2f}%")

volume_over_time_query = """
SELECT DATE_TRUNC('day', TO_DATE(USAGE_DATE, 'DD/MM/YYYY')) AS day, COUNT(*) AS row_count
FROM CTRF_PROD.DATA_QUALITY.SAMPLE_DATA
GROUP BY day
ORDER BY day
"""
volume_over_time = run_query(volume_over_time_query, "Data Volume Over Time")
print("\n📅 Data Volume Over Time (Rows per Day):")
for row in volume_over_time:
    print(row)

rolling_avg_query = """
SELECT day, AVG(TOTAL_MB_CHARGED) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg
FROM (
    SELECT DATE_TRUNC('day', TO_DATE(USAGE_DATE, 'DD/MM/YYYY')) AS day, TOTAL_MB_CHARGED
    FROM CTRF_PROD.DATA_QUALITY.SAMPLE_DATA
)
ORDER BY day
LIMIT 10
"""
rolling_avg = run_query(rolling_avg_query, "7-Day Rolling Average of TOTAL_MB_CHARGED")
print("\n📈 7-Day Rolling Average (First 10 Days):")
for row in rolling_avg:
    print(row)

outliers_query = """
SELECT * 
FROM CTRF_PROD.DATA_QUALITY.SAMPLE_DATA
WHERE TOTAL_COST > (SELECT AVG(TOTAL_COST) + 3 * STDDEV(TOTAL_COST) FROM CTRF_PROD.DATA_QUALITY.SAMPLE_DATA)
LIMIT 10
"""
outliers = run_query(outliers_query, "Potential Outliers (High TOTAL_COST)")
print("\n⚠️ Potential Outliers (High TOTAL_COST):")
for row in outliers:
    print(row)

