"""
Synthetic Event Generator for CloudDocs SaaS Analytics
Generates realistic user behavior events for testing the pipeline.

Author: Vladislav Laboga
Date: January 2026
Version: 1.0

This module creates synthetic SaaS product events that simulate real user behavior
for testing and demonstration purposes. Events follow realistic distributions
based on actual SaaS analytics patterns.

Key Features:
- Realistic event type distribution (document edits, logins, feature usage)
- User activity patterns based on time of day (morning/afternoon spikes)
- Geographic IP generation for geographic distribution
- Platform detection (web, mobile, desktop, API)
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from typing import Generator
import psycopg2
from psycopg2.extras import execute_values

# ================================================
# GLOBAL CONFIGURATION
# ================================================

# Configuration
NUM_USERS = 500
NUM_DOCUMENTS = 2000
EVENTS_PER_DAY = 10000

# Event type weights (realistic distribution based on typical SaaS behavior)
# document_edited: Most common (35%) - users actively work on documents
# feature_used: High frequency (20%) - users explore features
# user_login: Medium frequency (15%) - daily active user sessions
# document_created: Lower frequency (10%) - new content creation
# document_shared: Low frequency (8%) - collaboration sharing
# user_logout: Low frequency (5%) - session completion
# subscription events: Very low frequency (6%) - business operations
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

# Platform distribution (web > mobile > desktop > API)
PLATFORMS = ["web", "mobile", "desktop", "api"]
PLATFORM_WEIGHTS = [60, 25, 10, 5]

# User plan distribution (free tier dominates)
PLANS = ["free", "pro", "enterprise"]
PLAN_WEIGHTS = [70, 25, 5]

# Product features for feature_used events
# These represent different capabilities of the SaaS product
FEATURES = [
    {
        "id": "real_time_collab",
        "name": "Real-time Collaboration",
        "category": "collaboration",
        "premium": True,
    },
    {
        "id": "comments",
        "name": "Comments",
        "category": "collaboration",
        "premium": False,
    },
    {
        "id": "version_history",
        "name": "Version History",
        "category": "editing",
        "premium": True,
    },
    {
        "id": "export_pdf",
        "name": "Export to PDF",
        "category": "editing",
        "premium": False,
    },
    {"id": "templates", "name": "Templates", "category": "editing", "premium": False},
    {
        "id": "cloud_storage",
        "name": "Cloud Storage",
        "category": "storage",
        "premium": False,
    },
    {
        "id": "advanced_search",
        "name": "Advanced Search",
        "category": "analytics",
        "premium": True,
    },
    {
        "id": "team_analytics",
        "name": "Team Analytics",
        "category": "analytics",
        "premium": True,
    },
]


class EventGenerator:
    """
    Generates synthetic SaaS product events for testing the analytics pipeline.

    This class creates realistic user behavior patterns including:
    - Document creation, editing, sharing, and deletion
    - User login/logout sessions
    - Feature exploration and usage
    - Subscription lifecycle events
    - Geographic platform distribution

    Attributes:
        users (list): Pre-generated user pool with profiles
        documents (list): Pre-generated document pool
        sessions (dict): Active user sessions for session tracking
        seed (int): Random seed for reproducibility
    """

    def __init__(self, seed: int = 42):
        """
        Initialize the event generator.

        Args:
            seed (int): Random seed for reproducible event generation
        """
        random.seed(seed)
        self.users = self._generate_users()
        self.documents = self._generate_documents()
        self.sessions = {}  # Active sessions per user

    def _generate_users(self) -> list[dict]:
        """
        Pre-generate user pool with realistic user profiles.

        Creates users with different signup dates, plans, and activity levels.
        Users are assigned to plans based on realistic distribution (70% free, 25% pro, 5% enterprise).

        Returns:
            list: List of user dictionaries with profile information
        """
        users = []
        base_date = datetime(2024, 1, 1)  # Start date for user base

        for i in range(NUM_USERS):
            signup_offset = random.randint(0, 365)  # Users signed up throughout 2024
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
        """
        Pre-generate document pool for event generation.

        Creates documents owned by users, with realistic creation timing
        (documents created shortly after user signup).

        Returns:
            list: List of document dictionaries with metadata
        """
        documents = []
        for i in range(NUM_DOCUMENTS):
            owner = random.choice(self.users)
            documents.append(
                {
                    "document_id": f"doc_{uuid.uuid4().hex[:12]}",
                    "owner_user_id": owner["user_id"],
                    "title": f"Document {i}",
                    "created_at": owner["signup_date"]
                    + timedelta(days=random.randint(0, 30)),
                }
            )
        return documents

    def _get_session_id(self, user_id: str) -> str:
        """
        Get or create session ID for a user.

        Users maintain sessions with 90% persistence rate (10% chance of new session per event).
        This simulates real browser session behavior.

        Args:
            user_id (str): User identifier

        Returns:
            str: Session ID for the user
        """
        if user_id not in self.sessions or random.random() < 0.1:
            self.sessions[user_id] = f"ses_{uuid.uuid4().hex[:12]}"
        return self.sessions[user_id]

    def generate_event(self, event_date: datetime) -> dict:
        """
        Generate a single synthetic event with realistic properties.

        Events follow realistic distributions for event type, user, platform, and timing.
        Each event includes event metadata and event-specific properties.

        Args:
            event_date (datetime): Date for the event (time will be randomized)

        Returns:
            dict: Event dictionary with all required fields for the pipeline
        """
        # Select event type based on weighted distribution
        event_type = random.choices(
            list(EVENT_WEIGHTS.keys()), weights=list(EVENT_WEIGHTS.values())
        )[0]

        # Select random user from pool
        user = random.choice(self.users)

        # Select platform based on distribution
        platform = random.choices(PLATFORMS, weights=PLATFORM_WEIGHTS)[0]

        # Generate realistic time-of-day distribution
        # Morning: 6-11, Afternoon: 12-17, Evening: 18-23, Night: 0-5
        hour = random.choices(
            range(24),
            weights=[
                1,  # 0-1: Very low
                1,  # 1-2: Very low
                1,  # 2-3: Very low
                1,  # 3-4: Very low
                1,  # 4-5: Very low
                2,  # 5-6: Early morning
                3,  # 6-7: Morning spike
                5,  # 7-8: Peak morning
                8,  # 8-9: Peak morning
                10,  # 9-10: Peak morning
                10,  # 10-11: Peak morning
                9,  # 11-12: Midday
                7,  # 12-13: Afternoon start
                8,  # 13-14: Afternoon
                10,  # 14-15: Afternoon peak
                10,  # 15-16: Afternoon peak
                9,  # 16-17: Afternoon
                8,  # 17-18: Late afternoon
                6,  # 18-19: Evening start
                5,  # 19-20: Evening
                4,  # 20-21: Evening decline
                3,  # 21-22: Late evening
                2,  # 22-23: Very low
                1,  # 23-0: Night
            ],
        )[0]

        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        # Create timestamp for the event
        event_time = event_date.replace(hour=hour, minute=minute, second=second)

        # Build event
        event = {
            "event_id": f"evt_{uuid.uuid4().hex}",
            "event_type": event_type,
            "event_timestamp": event_time.isoformat() + "Z",
            "user_id": user["user_id"],
            "session_id": self._get_session_id(user["user_id"]),
            "properties": {},
            "context": {
                "platform": platform,
                "ip_address": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
                "user_agent": f"Mozilla/5.0 ({platform})",
            },
        }

        # Add event-specific properties based on event type
        if event_type in [
            "document_edited",
            "document_created",
            "document_deleted",
            "document_shared",
        ]:
            # Document-related events
            doc = random.choice(self.documents)
            event["properties"]["document_id"] = doc["document_id"]

            if event_type == "document_edited":
                # Edit duration: 10 seconds to 1 hour
                event["properties"]["edit_duration_sec"] = random.randint(10, 3600)
                # Characters added: 0 to 5000
                event["properties"]["characters_added"] = random.randint(0, 5000)

        elif event_type == "feature_used":
            # Feature exploration events
            feature = random.choice(FEATURES)
            event["properties"]["feature_id"] = feature["id"]
            event["properties"]["feature_name"] = feature["name"]
            event["properties"]["duration_sec"] = random.randint(
                5, 300
            )  # 5 seconds to 5 minutes

        elif event_type in ["subscription_started", "subscription_upgraded"]:
            # Subscription lifecycle events
            event["properties"]["plan"] = random.choice(["pro", "enterprise"])
            event["properties"]["billing_cycle"] = random.choice(["monthly", "annual"])

        elif event_type == "subscription_cancelled":
            # User cancellation events with reason
            event["properties"]["reason"] = random.choice(
                ["too_expensive", "not_using", "competitor", "other"]
            )

        return event

    def generate_day(
        self, event_date: datetime, num_events: int = EVENTS_PER_DAY
    ) -> Generator[dict, None, None]:
        """
        Generate all events for a single day using a generator.

        This generator yields events one at a time, which is memory-efficient
        for generating large numbers of events.

        Args:
            event_date (datetime): Date for the events (time will be randomized)
            num_events (int): Number of events to generate (default: EVENTS_PER_DAY)

        Yields:
            dict: Individual event dictionaries
        """
        for _ in range(num_events):
            yield self.generate_event(event_date)


def load_to_raw(events: list[dict], batch_id: str, db_config: dict) -> int:
    """
    Load events to raw.events table using PostgreSQL bulk insert.

    This function performs an idempotent bulk insert using execute_values
    for optimal performance with large event batches.

    Args:
        events (list): List of event dictionaries to insert
        batch_id (str): Batch identifier for tracking
        db_config (dict): PostgreSQL connection configuration

    Returns:
        int: Number of events successfully inserted
    """
    # Establish database connection
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    # Prepare data for bulk insert: convert events to tuples
    values = [(e["event_id"], json.dumps(e), batch_id) for e in events]

    # Idempotent insert: skip duplicates using ON CONFLICT
    # This ensures the same event_id won't cause errors on re-runs
    insert_sql = """
        INSERT INTO raw.events (event_id, raw_payload, batch_id)
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING
    """

    # Execute bulk insert
    execute_values(cur, insert_sql, values)
    inserted = cur.rowcount

    # Commit transaction
    conn.commit()

    # Clean up
    cur.close()
    conn.close()

    return inserted


def main():
    """
    Main entry point for event generation script.

    This function provides CLI argument parsing and orchestrates the event
    generation process for a specified date range. Supports dry-run mode for
    testing without loading to database.

    Example usage:
        python generate_current_events.py --start-date 2024-01-01 --end-date 2024-01-31
        python generate_current_events.py --start-date 2024-01-01 --events-per-day 5000 --dry-run
    """
    import argparse

    # Setup argument parser
    parser = argparse.ArgumentParser(
        description="Generate synthetic SaaS analytics events for testing the pipeline"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2024-01-01",
        help="Start date for event generation (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="2024-01-31",
        help="End date for event generation (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--events-per-day",
        type=int,
        default=EVENTS_PER_DAY,
        help="Number of events to generate per day",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print sample events without loading to database",
    )
    args = parser.parse_args()

    # Database configuration (default to localhost)
    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "saas_analytics",
        "user": "dataeng",
        "password": "secure_password_123",
    }

    # Initialize event generator with seed for reproducibility
    generator = EventGenerator(seed=42)

    # Parse date range
    start = datetime.strptime(args.start_date, "%Y-%m-%d")
    end = datetime.strptime(args.end_date, "%Y-%m-%d")

    # Process each day in the range
    current = start
    total_loaded = 0

    while current <= end:
        # Generate batch ID for tracking
        batch_id = f"batch_{current.strftime('%Y%m%d')}_{uuid.uuid4().hex[:8]}"

        # Generate events for the day
        events = list(generator.generate_day(current, args.events_per_day))

        # Dry run mode: print sample event without loading
        if args.dry_run:
            print(f"{current.date()}: Generated {len(events)} events")
            print(json.dumps(events[0], indent=2))
            print(f"{'-' * 50}")
        else:
            # Load events to database
            loaded = load_to_raw(events, batch_id, db_config)
            total_loaded += loaded
            print(f"{current.date()}: Loaded {loaded} events (batch: {batch_id})")

        # Move to next day
        current += timedelta(days=1)

    # Summary
    if not args.dry_run:
        print(f"\n{'=' * 50}")
        print(f"Total events loaded: {total_loaded}")
        print(f"Date range: {start.date()} to {end.date()}")


if __name__ == "__main__":
    main()
