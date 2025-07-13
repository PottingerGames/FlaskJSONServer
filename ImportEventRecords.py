import requests
import pyodbc
from datetime import datetime

API_KEY = "e7f13ab2-85a7-4d92-80c3-bf04cd519be3"
BASE_URL = "https://flaskjsonserver.onrender.com"

conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=DESKTOP-K9VSNBN\\SQLEXPRESS;"
    "Database=MinesNMayhem;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

def fetch_logs(endpoint):
    headers = {"x-api-key": API_KEY}
    response = requests.get(f"{BASE_URL}/{endpoint}", headers=headers)
    response.raise_for_status()
    return response.json()

def insert_item_events(events):
    for event in events:
        raw_date = event.get("received_at")
        try:
            # Try parsing ISO8601 datetime string to Python datetime object
            dt = datetime.fromisoformat(raw_date)
        except Exception as e:
            print(f"Error parsing date '{raw_date}': {e}")
            continue
        cursor.execute("SELECT COUNT(*) FROM [Fact].[ItemEvents] WHERE ReceivedAt = ?", (dt,))
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                INSERT INTO [Fact].[ItemEvents] (
                    GameVersion, SteamID64, RunId, ReceivedAt, Level, Stage, EventType,
                    TreasureCode, PotionCode, ElixirCode, RelicCode, ChestCode, TokenCode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event.get("GameVersion"),
                event.get("SteamID64"),
                event.get("RunId"),
                dt,
                event.get("Level"),
                event.get("Stage"),
                event.get("EventType"),
                event.get("TreasureCode"),
                event.get("PotionCode"),
                event.get("ElixirCode"),
                event.get("RelicCode"),
                event.get("ChestCode"),
                event.get("TokenCode"),
            ))
        else:
            print(f"Skipped duplicate ItemEvent at {event.get('received_at')}")
    conn.commit()

def insert_stage_events(events):
    for event in events:
        raw_date = event.get("received_at")
        try:
            # Try parsing ISO8601 datetime string to Python datetime object
            dt = datetime.fromisoformat(raw_date)
        except Exception as e:
            print(f"Error parsing date '{raw_date}': {e}")
            continue
        cursor.execute("SELECT COUNT(*) FROM [Fact].[StageEvent] WHERE ReceivedAt = ?", (dt,))
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                INSERT INTO [Fact].[StageEvent] (
                    GameVersion, SteamID64, RunId, ReceivedAt, Level, Stage,
                    BossCode, Victory, Score, TurnsTaken
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event.get("GameVersion"),
                event.get("SteamID64"),
                event.get("RunId"),
                dt,
                event.get("Level"),
                event.get("Stage"),
                event.get("BossCode"),
                event.get("Victory"),
                event.get("Score"),
                event.get("TurnsTaken"),
            ))
        else:
            print(f"Skipped duplicate StageEvent at {event.get('received_at')}")
    conn.commit()

def insert_error_events(events):
    for event in events:
        raw_date = event.get("received_at")
        try:
            dt = datetime.fromisoformat(raw_date)
        except Exception as e:
            print(f"Error parsing date '{raw_date}': {e}")
            continue
        # Consider a more robust check for duplicates for errors,
        # perhaps a combination of ReceivedAt, Message, and StackTrace hash
        cursor.execute("SELECT COUNT(*) FROM [Fact].[ErrorEvents] WHERE ReceivedAt = ? AND Message = ?", (dt, event.get("Message")))
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                INSERT INTO [Fact].[ErrorEvents] (
                    GameVersion, SteamID64, RunId, ReceivedAt, ErrorType, Message, StackTrace
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                event.get("GameVersion"),
                event.get("SteamID64"),
                event.get("RunId"),
                dt,
                event.get("ErrorType"),
                event.get("Message"),
                event.get("StackTrace"),
            ))
        else:
            print(f"Skipped potential duplicate ErrorEvent at {event.get('received_at')} with message: {event.get('Message')}")
    conn.commit()

def main():
    item_events = fetch_logs("get-item-logs")
    print(f"Fetched {len(item_events)} item logs")
    insert_item_events(item_events)

    stage_events = fetch_logs("get-stage-logs")
    print(f"Fetched {len(stage_events)} stage logs")
    insert_stage_events(stage_events)

    error_events = fetch_logs("get-error-logs")
    print(f"Fetched {len(error_events)} error logs")
    insert_error_events(error_events)

if __name__ == "__main__":
    main()
