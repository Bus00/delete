
import sqlite3
import pandas as pd


def save_risk_report_to_db(
    csv_path="data/processed/risk_report.csv",
    db_path="data/processed/deletewatch.db"
):

    print("Connecting to SQLite database...")

    conn = sqlite3.connect(db_path)
    df = pd.read_csv(csv_path)

    df.to_sql(
        name="risk_report",
        con=conn,
        if_exists="replace",
        index=False
    )

    conn.commit()
    conn.close()

    print("✅ Risk report successfully saved to database.")


if __name__ == "__main__":
    save_risk_report_to_db()
