
import pandas as pd

def generate_report(csv_path="data/processed/risk_report.csv"):

    print("\n Generating Risk Report...\n")

    df = pd.read_csv(csv_path)

    top5 = df.sort_values("risk_score", ascending=False).head(5)

    print("Top 5 Risky Users:")
    print(top5[["user_id", "date", "risk_score", "risk_flag"]])

    print("\n Risk Distribution:")
    distribution = df["risk_flag"].value_counts()
    print(distribution)

    print("\n✅ Reporting completed.")


if __name__ == "__main__":
    generate_report()
