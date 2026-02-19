
import argparse
import pandas as pd


def validate_dataset(input_path: str):

    df = pd.read_csv(input_path)

    print("Running validation checks...\n")

    required_columns = [
        "event_id",
        "timestamp",
        "user_id",
        "entity",
        "status",
        "sensitivity_score",
        "response_time_ms"
    ]

    missing_cols = [col for col in required_columns if col not in df.columns]

    if missing_cols:
        print(f"❌ Missing columns: {missing_cols}")
    else:
        print("✅ Required columns present")

    if df["event_id"].is_unique:
        print("✅ event_id values are unique")
    else:
        print("❌ Duplicate event_id detected")

    null_counts = df.isnull().sum().sum()

    if null_counts == 0:
        print("✅ No null values detected")
    else:
        print(f"⚠️ Null values found: {null_counts}")

    if (df["response_time_ms"] < 0).any():
        print("❌ Negative response_time detected")
    else:
        print("✅ response_time_ms valid")

    if df["status"].nunique() == 1 and df["status"].iloc[0] == "SUCCESS":
        print("✅ Status values valid")
    else:
        print("⚠️ Unexpected status values detected")

    print("\nValidation complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate processed dataset.")
    parser.add_argument(
        "--input",
        default="data/processed/parsed_logs.csv"
    )

    args = parser.parse_args()

    validate_dataset(args.input)
