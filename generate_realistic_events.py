import sys

# Generates historical events (2024-01 → 2024-06) with growth + weekend reduction
# Loads directly into raw.events (for full history)
sys.path.append("src")
from ingestion.event_generator import EventGenerator, load_to_raw
from datetime import datetime, timedelta
import random

# DB config
db_config = {
    "host": "localhost",
    "port": 5432,
    "database": "saas_analytics",
    "user": "dataeng",
    "password": "secure_password_123",
}

generator = EventGenerator(seed=42)

start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 6, 30)

current = start_date
total_loaded = 0
day_count = 0

while current <= end_date:
    day_count += 1

    # Base events (grows over time)
    base_events = 8000 + (day_count * 10)  # From 8k to ~10k

    # Weekend reduction (Saturday/Sunday = 60% fewer)
    if current.weekday() in [5, 6]:  # Saturday=5, Sunday=6
        base_events = int(base_events * 0.4)

    # Random variation ±15%
    variation = random.uniform(0.85, 1.15)
    events_today = int(base_events * variation)

    # Generate events
    batch_id = f'batch_{current.strftime("%Y%m%d")}'
    events = list(generator.generate_day(current, events_today))

    # Load
    loaded = load_to_raw(events, batch_id, db_config)
    total_loaded += loaded

    weekday = current.strftime("%A")
    print(f"{current.date()} ({weekday}): {loaded:,} events")

    current += timedelta(days=1)

print(f"\n Total: {total_loaded:,} events over {day_count} days")
print(f" Average: {total_loaded // day_count:,} events/day")
