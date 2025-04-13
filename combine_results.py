import pandas as pd

def compare_and_combine_anomalies(parquet_files, output_txt):
    """
    Reads anomaly results from multiple parquet files, prints summary counts,
    combines the anomalies (dropping duplicates), and writes a summary with the
    combined results to a text file.
    
    Parameters:
      parquet_files (list of str): List of parquet file paths.
      output_txt (str): Path to the output text file.
    """
    # Load each parquet file into a DataFrame
    dataframes = []
    summary_lines = []
    
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            dataframes.append(df)
            summary_lines.append(f"File '{file}': {len(df)} anomalies detected.")
        except Exception as e:
            summary_lines.append(f"Error reading '{file}': {e}")
    
    # Combine the DataFrames (here we drop duplicates assuming they represent the same anomaly)
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True).drop_duplicates()
        summary_lines.append(f"\nCombined anomalies (after dropping duplicates): {len(combined_df)} rows.")
    else:
        combined_df = pd.DataFrame()
        summary_lines.append("\nNo data loaded from the provided parquet files.")
    
    # Write the summary and combined data to a text file
    with open(output_txt, 'w') as file:
        file.write("Anomaly Comparison Summary:\n")
        file.write("\n".join(summary_lines))
        file.write("\n\nCombined Anomalies Data:\n")
        file.write(combined_df.to_string(index=False))
    
    print(f"Combined anomaly results saved to '{output_txt}'.")

def main():
    # List of parquet files that contain anomaly detection results
    parquet_files = [
        "numeric_columns_hourly_80_anomalies_SUM_MB_41.parquet",
        "sample_80_anomalies_SUM_MB_sample.parquet",
        "numeric_columns_hourly_80_anomalies_SUM_MB.parquet"
    ]
    output_txt = "combined_anomalies_80.txt"
    compare_and_combine_anomalies(parquet_files, output_txt)

if __name__ == "__main__":
    main()
