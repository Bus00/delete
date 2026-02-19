
import json
import os
import yaml
import pandas as pd


def parse_logs(
    input_path="data/raw/raw_deletion_logs.json",
    output_path="data/processed/parsed_logs.csv",
    config_path="config.yaml"
):
    """
    Raw deletion loglarını okuyup analiz için uygun bir CSV dosyası üretir.
    """

    # Config dosyasını okumak için var
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # JSON'a logları yüklüyoruz
    with open(input_path, "r", encoding="utf-8") as f:
        logs = json.load(f)

    df = pd.DataFrame(logs)

    
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    
    df = df.dropna(subset=["timestamp"])

    
    df["response_time_ms"] = df["response_time_ms"].fillna(0).astype(int)

    # Entity bazlı hassasiyet puanı ekliyoruz
    sensitivity_map = config["sensitivity_levels"]
    df["sensitivity_score"] = df["entity"].map(sensitivity_map).fillna(0)

    
    df["hour"] = df["timestamp"].dt.hour
    df["date"] = df["timestamp"].dt.date

    # Mesai dışı kontrolü (9-18)
    start = config["thresholds"]["out_of_hours_start"]
    end = config["thresholds"]["out_of_hours_end"]

    df["is_out_of_hours"] = (df["hour"] >= start) | (df["hour"] <= end)

    # Gerekli kolon kontrolü
    required_columns = ["event_id", "timestamp", "user_id", "entity", "status"]

    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Çıktı klasörü yoksa oluştur
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_csv(output_path, index=False)

    print(f"Logs parsed and saved to {output_path}")
    print(f"Total valid records: {len(df)}")


if __name__ == "__main__":
    parse_logs()
