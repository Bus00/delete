import json
import os
import random
from datetime import datetime, timedelta
import uuid
import yaml

USERS = [
    {"user_id": "U1001", "name": "ali.yilmaz", "dept": "IT"},
    {"user_id": "U1002", "name": "ayse.demir", "dept": "HR"},
    {"user_id": "U1003", "name": "can.kaya", "dept": "Sales"},
    {"user_id": "U1004", "name": "zeynep.celik", "dept": "Finance"},
    {"user_id": "U9999", "name": "test.account", "dept": "External"},  # Bilinçli olarak spike üretiyoruzz
]

# Silinebilecek tablolarr
ENTITIES = [
    "system_logs",
    "user_sessions",
    "product_catalog",
    "customer_data",
    "financial_records"
]


def generate_ip():
    """Basit bir local IP üretir."""
    return f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}"


def generate_timestamp(base_date, hour=None):
    """
    Rastgele bir timestamp üretir.
    Saat verilmezse mesai saatleri arasında üretir.
    """
    if hour is None:
        hour = random.randint(9, 17)

    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    return base_date.replace(
        hour=hour,
        minute=minute,
        second=second
    ).isoformat()


def create_log_entry(user, entity, timestamp, config):
    """Tek bir log kaydı oluşturur."""
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": timestamp,
        "user_id": user["user_id"],
        "department": user["dept"],
        "entity": entity,
        "schema": config.get("schema", "public"),
        "action": "DELETE",
        "status": "SUCCESS",
        "source_ip": generate_ip(),
        "environment": config.get("environment", "production"),
        "response_time_ms": random.randint(20, 500),
    }


def generate_logs(config_path="config.yaml",
                  output_path="data/raw/raw_deletion_logs.json"):
    """Sentetik deletion loglarını üretir ve JSON olarak kaydeder."""

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    random.seed(config["simulation"]["random_seed"])

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    logs = []
    today = datetime.now()

    days = config["simulation"]["days"]
    min_events = config["simulation"]["min_daily_events"]
    max_events = config["simulation"]["max_daily_events"]
    mass_size = config["simulation"]["mass_deletion_size"]

    print("Sentetik log üretimi başladı...")

    # Normal günlük aktiviteler
    for day_offset in range(days):
        current_date = today - timedelta(days=day_offset)

        daily_event_count = random.randint(min_events, max_events)

        for _ in range(daily_event_count):
            user = random.choice(USERS[:-1])  # test.account hariç
            entity = random.choice(ENTITIES[:3])
            timestamp = generate_timestamp(current_date)

            logs.append(create_log_entry(user, entity, timestamp, config))

    # --- Aklıma Gelen Anomali Senaryoları ---

    # 1) HR çalışanı gece 3'te session silerse
    hr_user = USERS[1]
    logs.append(
        create_log_entry(
            hr_user,
            "user_sessions",
            generate_timestamp(today, hour=3),
            config
        )
    )

    # 2) Sales çalışanı finans verisi silerse
    sales_user = USERS[2]
    logs.append(
        create_log_entry(
            sales_user,
            "financial_records",
            generate_timestamp(today),
            config
        )
    )

    # 3) Test hesabı toplu customer_data silerse
    spike_user = USERS[-1]
    for _ in range(mass_size):
        logs.append(
            create_log_entry(
                spike_user,
                "customer_data",
                generate_timestamp(today),
                config
            )
        )

    # Kronolojik sıralı hali
    logs.sort(key=lambda x: x["timestamp"])

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(logs, f, indent=4, ensure_ascii=False)

    print(f"{len(logs)} adet log başarıyla üretildi.")


if __name__ == "__main__":
    generate_logs()
