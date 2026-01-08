import sys
sys.path.append('src')
from ingestion.event_generator import EventGenerator, load_to_raw
from datetime import datetime, timedelta
import random

db_config = {
    'host': 'localhost',
    'port': 5432,
    'database': 'saas_analytics',
    'user': 'dataeng',
    'password': 'secure_password_123',
}

generator = EventGenerator(seed=42)

# Von Juli 2024 bis Januar 2026
start_date = datetime(2024, 7, 1)
end_date = datetime(2026, 1, 8)

current = start_date
total_loaded = 0
day_count = 0

while current <= end_date:
    day_count += 1
    
    # Base events (wächst über Zeit)
    base_events = 9000 + (day_count * 5)
    
    # Wochenend-Reduktion
    if current.weekday() in [5, 6]:
        base_events = int(base_events * 0.4)
    
    # Zufällige Schwankung
    variation = random.uniform(0.85, 1.15)
    events_today = int(base_events * variation)
    
    # Events generieren
    batch_id = f'batch_{current.strftime("%Y%m%d")}'
    events = list(generator.generate_day(current, events_today))
    
    # Laden
    loaded = load_to_raw(events, batch_id, db_config)
    total_loaded += loaded
    
    if day_count % 30 == 0:  # Nur jeden 30. Tag ausgeben
        print(f'{current.date()}: {loaded:,} events (Total: {total_loaded:,})')
    
    current += timedelta(days=1)

print(f'\n✅ Total: {total_loaded:,} events über {day_count} Tage')
