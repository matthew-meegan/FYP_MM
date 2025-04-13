import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

stage_processing_100pct = {
    "File Received": 250,
    "Ingest":        625,
    "Transform":     375,
    "Supplement":    1125
}

stage_correction_cost = {
    "File Received": 210,
    "Ingest":        140,
    "Transform":     80,
    "Supplement":    300
}

sample_sizes = [1, 5, 15, 30, 50, 80, 100]

# ARIMA(MB) anomaly counts:
A_MB = np.array([11, 15, 16, 19, 20, 21, 22])
# ARIMA(Sessions) anomaly counts:
A_Sess = np.array([5, 9, 9, 10, 11, 14, 14])
# IF anomalies (fixed at 1% sample), assumed constant:
IF_anomalies = 2

N = 50

def linear_cost_100pct(cost_100pct, sample_pct):
    return cost_100pct * (sample_pct / 100.0)

results_by_stage = {}

for stage in stage_processing_100pct.keys():
    # Initialize cost matrix (rows: MB sample sizes, cols: Sessions sample sizes)
    cost_matrix = np.zeros((len(sample_sizes), len(sample_sizes)))
    
    # For each stage, get the 100% processing cost and correction cost.
    base_stage_cost = stage_processing_100pct[stage]
    corr_cost = stage_correction_cost[stage]
    
    # Loop over MB (row) and Sessions (column) sample sizes.
    for i, p in enumerate(sample_sizes):
        # MB model cost scaled to p%:
        cost_MB = linear_cost_100pct(base_stage_cost, p)
        anomalies_MB = A_MB[i]
        for j, q in enumerate(sample_sizes):
            # Sessions model cost scaled to q%:
            cost_Sess = linear_cost_100pct(base_stage_cost, q)
            anomalies_Sess = A_Sess[j]
            # IF is fixed at 1% sample:
            cost_IF = linear_cost_100pct(base_stage_cost, 1)
            
            # Compute total anomalies caught (union of MB, Sessions and IF)
            total_anomalies_caught = anomalies_MB + anomalies_Sess + IF_anomalies
            
            missed = N - total_anomalies_caught
            if missed < 0:
                missed = 0
            
            # Total processing cost: Sum of each model's cost.
            total_processing = cost_MB + cost_Sess + cost_IF
            
           
            total_cost = total_processing + missed * corr_cost
            
            
            cost_matrix[i, j] = total_cost
    
    results_by_stage[stage] = cost_matrix

for stage, matrix in results_by_stage.items():
    plt.figure(figsize=(8, 6))
    ax = sns.heatmap(
        matrix,
        annot=True,
        fmt=".1f",
        cmap="viridis",
        xticklabels=[f"{q}%" for q in sample_sizes],
        yticklabels=[f"{p}%" for p in sample_sizes]
    )
    ax.set_xlabel("ARIMA (Sessions) Sample Size", fontsize=14)
    ax.set_ylabel("ARIMA (MB) Sample Size", fontsize=14)
    ax.set_title(f"{stage} - Cost Matrix (IF fixed at 1%)", fontsize=16)
    plt.tight_layout()
    plt.show()
