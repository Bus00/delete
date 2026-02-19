
import argparse
import pandas as pd
import numpy as np
from pathlib import Path

def ensure_directory(path: str):
    Path(path).mkdir(parents=True, exist_ok=True)

def calculate_risk(input_path: str, output_path: str):

    df = pd.read_csv(input_path, parse_dates=["timestamp"])

   
    daily_counts = (
        df.groupby(["user_id", "date"])
        .size()
        .reset_index(name="daily_delete_count")
    )

    
    user_stats = (
        daily_counts.groupby("user_id")["daily_delete_count"]
        .agg(["mean", "std"])
        .reset_index()
        .rename(columns={"mean": "user_mean", "std": "user_std"})
    )

    user_stats["user_std"] = user_stats["user_std"].replace(0, 1).fillna(1)

    daily_counts = daily_counts.merge(user_stats, on="user_id", how="left")

    
    daily_counts["z_score"] = (
        (daily_counts["daily_delete_count"] - daily_counts["user_mean"])
        / daily_counts["user_std"]
    )

    daily_counts["z_score"] = daily_counts["z_score"].abs()

   
    daily_sensitivity = (
        df.groupby(["user_id", "date"])["sensitivity_score"]
        .mean()
        .reset_index(name="avg_sensitivity")
    )

    daily_counts = daily_counts.merge(
        daily_sensitivity, on=["user_id", "date"], how="left"
    )

   
    daily_out_of_hours = (
        df.groupby(["user_id", "date"])["is_out_of_hours"]
        .max()
        .reset_index(name="has_out_of_hours")
    )

    daily_counts = daily_counts.merge(
        daily_out_of_hours, on=["user_id", "date"], how="left"
    )

    daily_counts["time_multiplier"] = np.where(
        daily_counts["has_out_of_hours"], 1.5, 1.0
    )

   
    burst_counts = (
        df.groupby(["user_id", "timestamp"])
        .size()
        .reset_index(name="burst_count")
    )

    burst_counts = burst_counts[burst_counts["burst_count"] > 1]

    daily_burst = (
        burst_counts.groupby("user_id")["burst_count"]
        .max()
        .reset_index()
    )

    daily_counts = daily_counts.merge(
        daily_burst, on="user_id", how="left"
    )

    daily_counts["burst_count"] = daily_counts["burst_count"].fillna(0)

   
    daily_counts["burst_score"] = np.log1p(daily_counts["burst_count"]) * 5

    daily_counts["risk_score"] = (
        daily_counts["z_score"]
        * daily_counts["avg_sensitivity"]
        * daily_counts["time_multiplier"]
    ) + daily_counts["burst_score"]

    
    daily_counts["risk_flag"] = np.select(
        [
            daily_counts["risk_score"] > 15,
            daily_counts["risk_score"] > 7
        ],
        ["HIGH", "MEDIUM"],
        default="LOW"
    )

    ensure_directory(Path(output_path).parent)

    daily_counts = daily_counts.sort_values(
        "risk_score", ascending=False
    )

    daily_counts.to_csv(output_path, index=False)

    print(f" Risk report saved to {output_path}")
    print("\n Top 5 Risky Users:")
    print(daily_counts.head())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run risk analysis.")
    parser.add_argument(
        "--input",
        default="data/processed/parsed_logs.csv"
    )
    parser.add_argument(
        "--output",
        default="data/processed/risk_report.csv"
    )

    args = parser.parse_args()

    calculate_risk(args.input, args.output)
