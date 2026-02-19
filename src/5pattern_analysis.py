
import pandas as pd

def run_pattern_analysis(
    csv_path="data/processed/parsed_logs.csv",
    work_start=9,
    work_end=18
):

    
    df = pd.read_csv(csv_path)

    
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["hour"] = df["timestamp"].dt.hour

    
    print(" Deletion Count by Hour:\n")

    hourly = df["hour"].value_counts().sort_index()

    for hour, count in hourly.items():
        print(f"{hour:02d}:00 → {count} deletions")

    peak_hour = hourly.idxmax()
    peak_count = hourly.max()

    
    print("\n Deletion Count by Entity:\n")

    entity_counts = df["entity"].value_counts()

    for entity, count in entity_counts.items():
        print(f"{entity} → {count} deletions")

    top_entity = entity_counts.idxmax()

    
    print("\n Average Sensitivity Score by Entity:\n")

    sensitivity = (
        df.groupby("entity")["sensitivity_score"]
        .mean()
        .sort_values(ascending=False)
    )

    for entity, score in sensitivity.items():
        print(f"{entity} → {score:.1f}")

    most_sensitive = sensitivity.idxmax()

    
    df["out_of_hours"] = (
        (df["hour"] < work_start) |
        (df["hour"] > work_end)
    )

    out_ratio = df["out_of_hours"].mean()

    print(
        f"\n Out-of-hours deletion ratio "
        f"(work hours assumed {work_start}:00–{work_end}:00): "
        f"{out_ratio:.2%}"
    )

   
    print("\n Net Summary\n")
    print(f"- Silmeler en çok {peak_hour:02d}:00 saatinde ({peak_count} deletions).")
    print(f"- En çok silinen veri türü: {top_entity}.")
    print(f"- En hassas veri türü: {most_sensitive}.")
    print(
        f"- Mesai dışı silme oranı "
        f"({work_start}:00–{work_end}:00 dışı): {out_ratio:.2%}."
    )

    print("\n✅ Pattern analysis completed.")


if __name__ == "__main__":
    run_pattern_analysis()
