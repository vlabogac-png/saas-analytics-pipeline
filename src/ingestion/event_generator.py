"""
Synthetic Event Generator for CloudDocs SaaS Analytics
Generates realistic user behavior events for testing the pipeline.
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from typing import Generator
import psycopg2
from psycopg2.extras import execute_values

# Configuration
NUM_USERS = 500
NUM_DOCUMENTS = 2000
EVENTS_PER_DAY = 10000

# Event type weights (realistic distribution)
EVENT_WEIGHTS = {
    "document_edited": 35,
    "document_created": 10,
    "user_login": 15,
    "feature_used": 20,
    "document_shared": 8,
    "user_logout": 5,
    "subscription_started": 2,
    "subscription_upgraded": 3,
    "subscription_cancelled": 1,
    "document_deleted": 1,
}

PLATFORMS = ["web", "mobile", "desktop", "api"]
PLATFORM_WEIGHTS = [60, 25, 10, 5]

PLANS = ["free", "pro", "enterprise"]
PLAN_WEIGHTS = [70, 25, 5]

FEATURES = [
    {"id": "real_time_collab", "name": "Real-time Collaboration", "category": "collaboration", "premium": True},
    {"id": "comments", "name": "Comments", "category": "collaboration", "premium": False},
    {"id": "version_history", "name": "Version History", "category": "editing", "premium": True},
    {"id": "export_pdf", "name": "Export to PDF", "category": "editing", "premium": False},
    {"id": "templates", "name": "Templates", "category": "editing", "premium": False},
    {"id": "cloud_storage", "name": "Cloud Storage", "category": "storage", "premium": False},
    {"id": "advanced_search", "name": "Advanced Search", "category": "analytics", "premium": True},
    {"id": "team_analytics", "name": "Team Analytics", "category": "analytics", "premium": True},
]


class EventGenerator:
    """Generates synthetic SaaS product events."""

    def __init__(self, seed: int = 42):
        random.seed(seed)
        self.users = self._generate_users()
        self.documents = self._generate_documents()
        self.sessions = {}  # Active sessions per user

    def _generate_users(self) -> list[dict]:
        """Pre-generate user pool."""
        users = []
        base_date = datetime(2024, 1, 1)

        for i in range(NUM_USERS):
            signup_offset = random.randint(0, 365)
            plan = random.choices(PLANS, weights=PLAN_WEIGHTS)[0]

            users.append(
                {
                    "user_id": f"usr_{uuid.uuid4().hex[:12]}",
                    "email": f"user{i}@example.com",
                    "signup_date": base_date + timedelta(days=signup_offset),
                    "plan": plan,
                    "activity_level": random.choice(["high", "medium", "low"]),
                }
            )
        return users

    def _generate_documents(self) -> list[dict]:
        """Pre-generate document pool."""
        documents = []
        for i in range(NUM_DOCUMENTS):
            owner = random.choice(self.users)
            documents.append(
                {
                    "document_id": f"doc_{uuid.uuid4().hex[:12]}",
                    "owner_user_id": owner["user_id"],
                    "title": f"Document {i}",
                    "created_at": owner["signup_date"] + timedelta(days=random.randint(0, 30)),
                }
            )
        return documents

    def _get_session_id(self, user_id: str) -> str:
        """Get or create session for user."""
        if user_id not in self.sessions or random.random() < 0.1:
            self.sessions[user_id] = f"ses_{uuid.uuid4().hex[:12]}"
        return self.sessions[user_id]

    def generate_event(self, event_date: datetime) -> dict:
        """Generate a single random event."""
        event_type = random.choices(list(EVENT_WEIGHTS.keys()), weights=list(EVENT_WEIGHTS.values()))[0]

        user = random.choice(self.users)
        platform = random.choices(PLATFORMS, weights=PLATFORM_WEIGHTS)[0]

        # Random time within the day
        hour = random.choices(
            range(24),
            weights=[
                1,
                1,
                1,
                1,
                1,
                2,  # 0-5: low activity
                3,
                5,
                8,
                10,
                10,
                9,  # 6-11: morning ramp
                7,
                8,
                10,
                10,
                9,
                8,  # 12-17: afternoon
                6,
                5,
                4,
                3,
                2,
                1,  # 18-23: evening decline
            ],
        )[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        event_timestamp = event_date.replace(hour=hour, minute=minute, second=second)

        # Build event
        event = {
            "event_id": f"evt_{uuid.uuid4().hex}",
            "event_type": event_type,
            "event_timestamp": event_timestamp.isoformat() + "Z",
            "user_id": user["user_id"],
            "session_id": self._get_session_id(user["user_id"]),
            "properties": {},
            "context": {
                "platform": platform,
                "ip_address": f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
                "user_agent": f"Mozilla/5.0 ({platform})",
            },
        }

        # Add event-specific properties
        if event_type in ["document_edited", "document_created", "document_deleted", "document_shared"]:
            doc = random.choice(self.documents)
            event["properties"]["document_id"] = doc["document_id"]

            if event_type == "document_edited":
                event["properties"]["edit_duration_sec"] = random.randint(10, 3600)
                event["properties"]["characters_added"] = random.randint(0, 5000)

        elif event_type == "feature_used":
            feature = random.choice(FEATURES)
            event["properties"]["feature_id"] = feature["id"]
            event["properties"]["feature_name"] = feature["name"]
            event["properties"]["duration_sec"] = random.randint(5, 300)

        elif event_type in ["subscription_started", "subscription_upgraded"]:
            event["properties"]["plan"] = random.choice(["pro", "enterprise"])
            event["properties"]["billing_cycle"] = random.choice(["monthly", "annual"])

        elif event_type == "subscription_cancelled":
            event["properties"]["reason"] = random.choice(["too_expensive", "not_using", "competitor", "other"])

        return event

    def generate_day(self, event_date: datetime, num_events: int = EVENTS_PER_DAY) -> Generator[dict, None, None]:
        """Generate all events for a single day."""
        for _ in range(num_events):
            yield self.generate_event(event_date)


def load_to_raw(events: list[dict], batch_id: str, db_config: dict) -> int:
    """Load events to raw.events table."""
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    # Prepare data for bulk insert
    values = [(e["event_id"], json.dumps(e), batch_id) for e in events]

    # Idempotent insert (skip duplicates)
    insert_sql = """
        INSERT INTO raw.events (event_id, raw_payload, batch_id)
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING
    """

    execute_values(cur, insert_sql, values)
    inserted = cur.rowcount

    conn.commit()
    cur.close()
    conn.close()

    return inserted


def main():
    """Generate and load events for a date range."""
    import argparse

    parser = argparse.ArgumentParser(description="Generate synthetic events")
    parser.add_argument("--start-date", type=str, default="2024-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default="2024-01-31", help="End date (YYYY-MM-DD)")
    parser.add_argument("--events-per-day", type=int, default=EVENTS_PER_DAY, help="Events per day")
    parser.add_argument("--dry-run", action="store_true", help="Print events without loading")
    args = parser.parse_args()

    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "saas_analytics",
        "user": "dataeng",
        "password": "secure_password_123",
    }

    generator = EventGenerator(seed=42)

    start = datetime.strptime(args.start_date, "%Y-%m-%d")
    end = datetime.strptime(args.end_date, "%Y-%m-%d")

    current = start
    total_loaded = 0

    while current <= end:
        batch_id = f'batch_{current.strftime("%Y%m%d")}_{uuid.uuid4().hex[:8]}'
        events = list(generator.generate_day(current, args.events_per_day))

        if args.dry_run:
            print(f"{current.date()}: Generated {len(events)} events")
            print(json.dumps(events[0], indent=2))
        else:
            loaded = load_to_raw(events, batch_id, db_config)
            total_loaded += loaded
            print(f"{current.date()}: Loaded {loaded} events (batch: {batch_id})")

        current += timedelta(days=1)

    if not args.dry_run:
        print(f"\nTotal events loaded: {total_loaded}")


if __name__ == "__main__":
    main()
