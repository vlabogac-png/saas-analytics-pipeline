import sys

# Generates recent events for the last month with growth + weekend reduction
# Loads directly into raw.events (for demo / dashboard)
sys.path.append("src")
from ingestion.event_generator import EventGenerator, load_to_raw
from datetime import datetime, timedelta
import random

db_config = {
    "host": "localhost",
    "port": 5432,
    "database": "saas_analytics",
    "user": "dataeng",
    "password": "secure_password_123",
}

generator = EventGenerator(seed=42)

# Last 30 days (inclusive)
end_date = datetime.utcnow().date()
start_date = end_date - timedelta(days=29)

current = datetime.combine(start_date, datetime.min.time())
end_date = datetime.combine(end_date, datetime.min.time())
total_loaded = 0
day_count = 0

while current <= end_date:
    day_count += 1

    # Base events (grows over time)
    base_events = 9000 + (day_count * 5)

    # Weekend reduction
    if current.weekday() in [5, 6]:
        base_events = int(base_events * 0.4)

    # Random variation
    variation = random.uniform(0.85, 1.15)
    events_today = int(base_events * variation)

    # Generate events
    batch_id = f'batch_{current.strftime("%Y%m%d")}'
    events = list(generator.generate_day(current, events_today))

    # Load
    loaded = load_to_raw(events, batch_id, db_config)
    total_loaded += loaded

    if day_count % 30 == 0:  # Print every 30th day only
        print(f"{current.date()}: {loaded:,} events (Total: {total_loaded:,})")

    current += timedelta(days=1)

print(f"\n✅ Total: {total_loaded:,} events over {day_count} days")
