import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import IsolationForest
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

plt.rcParams.update({'font.size': 16})
sns.set_context("talk")

def compare_feature_medians(df, numeric_cols):
   
    anomalies = df[df["anomaly"] == -1]
    normal = df[df["anomaly"] == 1]
    median_comparison = []
    for col in numeric_cols:
        median_anom = anomalies[col].median()
        median_norm = normal[col].median()
        diff = abs(median_anom - median_norm)
        median_comparison.append({
            "feature": col,
            "median_anomaly": median_anom,
            "median_normal": median_norm,
            "abs_diff": diff
        })
    return pd.DataFrame(median_comparison).sort_values("abs_diff", ascending=False)

def process_dataset(file_name, sample_label):
   
    print(f"\nProcessing dataset: {sample_label}")
    print(f"Loading data from {file_name} ...")
    try:
        df = pd.read_parquet(file_name, engine="pyarrow")
    except Exception as e:
        print(f"Error loading {file_name}: {e}")
        return None
    
    print(f"Data loaded. Shape: {df.shape}")
    
    print("Data types before conversion:")
    print(df.dtypes)
    
    if "USAGE_DATE" in df.columns:
        df["USAGE_DATE"] = pd.to_datetime(df["USAGE_DATE"], errors="coerce")
        df["USAGE_DATE"] = df["USAGE_DATE"].map(pd.Timestamp.toordinal)
    
    if "SESSION_HOUR" in df.columns:
        if not np.issubdtype(df["SESSION_HOUR"].dtype, np.number):
            df["SESSION_HOUR"] = pd.to_datetime(df["SESSION_HOUR"], errors="coerce").dt.hour
    
    print("Data types after conversion:")
    print(df.dtypes)
    
    # Scale the data.
    scaler = StandardScaler()
    df_scaled = pd.DataFrame(scaler.fit_transform(df), columns=df.columns)
    
    # Apply Isolation Forest
    iso_forest = IsolationForest(random_state=42, contamination="auto")
    iso_forest.fit(df_scaled)
    
    df["anomaly"] = iso_forest.predict(df_scaled)
    df["anomaly_score"] = iso_forest.decision_function(df_scaled)
    
    num_anomalies = (df["anomaly"] == -1).sum()
    total_rows = df.shape[0]
    anomaly_rate = num_anomalies / total_rows if total_rows > 0 else 0
    
    print(f"\nAnomaly detection complete for {sample_label}.")
    print(f"Number of anomalies found: {num_anomalies} out of {total_rows} rows ({anomaly_rate:.2%})")
    print("Anomaly label counts:")
    print(df["anomaly"].value_counts())
    
    anomalies = df[df["anomaly"] == -1]
    anomaly_details_file = f"Anomaly_Details_{sample_label}.txt"
    with open(anomaly_details_file, "w", encoding="utf-8") as f:
        f.write(f"Anomaly Details for dataset: {sample_label}\n")
        f.write(f"Total anomalies: {num_anomalies}\n\n")
        # Write out each row's details
        for idx, row in anomalies.iterrows():
            f.write(f"Row Index: {idx}\n")
            for col in df.columns:
                f.write(f"  {col}: {row[col]}\n")
            f.write("\n")
    print(f"Anomaly details saved to {anomaly_details_file}")
    
    normal_data = df[df["anomaly"] == 1]
    
    if "SESSION_HOUR" in df.columns:
        hourly_anomalies = anomalies.groupby("SESSION_HOUR").size().reset_index(name="anomaly_count")
        print("\n--- Anomaly Count by SESSION_HOUR ---")
        print(hourly_anomalies)
        plt.figure(figsize=(10, 6))
        sns.barplot(x="SESSION_HOUR", y="anomaly_count", data=hourly_anomalies, palette="coolwarm")
        plt.title(f"Anomaly Count by SESSION_HOUR ({sample_label})", fontsize=20)
        plt.xlabel("SESSION_HOUR", fontsize=18)
        plt.ylabel("Anomaly Count", fontsize=18)
        plt.xticks(fontsize=16)
        plt.yticks(fontsize=16)
        plt.show()
    
    if "COUNTRY" in df.columns and "SESSION_HOUR" in df.columns:
        country_hour_anomalies = anomalies.groupby(["COUNTRY", "SESSION_HOUR"]).size().reset_index(name="count")
        top_country_hour = country_hour_anomalies.sort_values("count", ascending=False).head(10)
        print("\n--- Top 10 Anomaly Groups by COUNTRY and SESSION_HOUR ---")
        print(top_country_hour)
    
    for col in ["TOTAL_MB_CHARGED", "TOTAL_SESSIONS"]:
        if col in df.columns:
            anomaly_median = anomalies[col].median()
            normal_median = normal_data[col].median()
            print(f"\nMedian {col} for anomalies: {anomaly_median}, for normal data: {normal_median}")
    
    numeric_cols = [col for col in df.columns if np.issubdtype(df[col].dtype, np.number)]
    median_comparison_df = compare_feature_medians(df, numeric_cols)
    print("\n--- Top 40 Feature Median Differences (Anomalies vs Normal) ---")
    print(median_comparison_df.head(40))
    
    #  PCA VISUALISATION 
    pca = PCA(n_components=2, random_state=42)
    df_pca = pca.fit_transform(df_scaled)
    df["pca_1"] = df_pca[:, 0]
    df["pca_2"] = df_pca[:, 1]
    
    plt.figure(figsize=(10, 8))
    sns.scatterplot(x="pca_1", y="pca_2", hue="anomaly", data=df,
                    palette={1: "blue", -1: "red"}, alpha=0.7)
    plt.title(f"PCA of Data with Isolation Forest Anomaly Labels ({sample_label})", fontsize=20)
    plt.xlabel("PCA Component 1", fontsize=18)
    plt.ylabel("PCA Component 2", fontsize=18)
    plt.legend(title="Anomaly\n(1 = normal, -1 = anomaly)", fontsize=16, title_fontsize=18)
    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
    plt.show()
    
    from sklearn import tree
    plt.figure(figsize=(20, 10))
    tree.plot_tree(iso_forest.estimators_[0],
                   feature_names=df.columns,
                   filled=True,
                   impurity=False,
                   rounded=True)
    plt.title("Visualization of the First Tree in the Isolation Forest", fontsize=20)
    plt.show()
    
    # Save the processed results to a new Parquet file
    output_file = f"IF_Results_{sample_label}.parquet"
    df.to_parquet(output_file, engine="pyarrow", index=False)
    print(f"Results saved to {output_file}")
    
    # Return summary metrics for comparison
    summary = {
        "sample": sample_label,
        "rows": total_rows,
        "anomaly_count": num_anomalies,
        "anomaly_rate": anomaly_rate
    }
    return summary

def main():
    datasets = {
        "Full": "IF_Ready_Data.parquet",
        #"15": "IF_Ready_Data_15.parquet",
        #"30": "IF_Ready_Data_30.parquet",
        #"50": "IF_Ready_Data_50.parquet",
        #"80": "IF_Ready_Data_80.parquet",
        #"05": "IF_Ready_Data_05.parquet",
        "01": "IF_Ready_Data_01.parquet"
    }
    
    summaries = []
    
    for label, file_name in datasets.items():
        summary = process_dataset(file_name, label)
        if summary:
            summaries.append(summary)
    
    summary_df = pd.DataFrame(summaries)
    print("\n--- Summary Comparison of Datasets ---")
    print(summary_df)
    
    # Plot comparison of anomaly rates for each dataset
    plt.figure(figsize=(10, 6))
    sns.barplot(x="sample", y="anomaly_rate", data=summary_df, palette="viridis")
    plt.title("Anomaly Rate Comparison Across Datasets", fontsize=20)
    plt.xlabel("Dataset Sample", fontsize=18)
    plt.ylabel("Anomaly Rate", fontsize=18)
    plt.ylim(0, summary_df["anomaly_rate"].max() * 1.1)
    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
    plt.show()
    
    # Plot comparison of anomaly counts for each dataset
    plt.figure(figsize=(10, 6))
    sns.barplot(x="sample", y="anomaly_count", data=summary_df, palette="magma")
    plt.title("Anomaly Count Comparison Across Datasets", fontsize=20)
    plt.xlabel("Dataset Sample", fontsize=18)
    plt.ylabel("Anomaly Count", fontsize=18)
    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
    plt.show()

if __name__ == "__main__":
    main()
