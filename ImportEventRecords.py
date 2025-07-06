# This will import the data from the JSON file in the flask
import pyodbc

conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=DESKTOP-K9VSNBN\SQLEXPRESS;"  # Adjust if your instance is named differently
    "Database=MinesNMayhem;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

# Function to import event records from a JSONL file into the database
import json

def read_jsonl(file_path):
    with open(file_path, 'r') as f:
        for line in f:
            yield json.loads(line)

for event in read_jsonl("logs_item.jsonl"):
    cursor.execute("""
    SELECT COUNT(*) FROM [Fact].[ItemEvents]
    WHERE ReceivedAt = ?
    """, (
        event.get("received_at"),
    ))
    if cursor.fetchone()[0] == 0:
        # safe to insert
        cursor.execute("""
            INSERT INTO [Fact].[ItemEvents] (
                GameVersion, SteamID64, RunId, ReceivedAt, Level, Stage, EventType,
                TreasureCode, PotionCode, ElixirCode, RelicCode, ChestCode, TokenCode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event.get("GameVersion"),
            event.get("SteamID64"),
            event.get("RunId"),
            event.get("received_at"),
            event.get("Level"),
            event.get("Stage"),
            event.get("EventType"),
            event.get("TreasureCode"),
            event.get("PotionCode"),
            event.get("ElixirCode"),
            event.get("RelicCode"),
            event.get("ChestCode"),
            event.get("TokenCode")
        ))
        print(f"Inserted ItemEvent at {event.get('received_at')}")
    else:
        print(f"Skipped duplicate ItemEvent at {event.get('received_at')}")
conn.commit()

for event in read_jsonl("logs_stage.jsonl"):
    cursor.execute("""
    SELECT COUNT(*) FROM [Fact].[StageEvent]
    WHERE ReceivedAt = ?
    """, (
        event.get("received_at"),
    ))
    if cursor.fetchone()[0] == 0:
        # safe to insert
        cursor.execute("""
            INSERT INTO [Fact].[StageEvent] (
                GameVersion, SteamID64, RunId, ReceivedAt, Level, Stage,
                BossCode, Victory, Score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event.get("GameVersion"),
            event.get("SteamID64"),
            event.get("RunId"),
            event.get("received_at"),
            event.get("Level"),
            event.get("Stage"),
            event.get("BossCode"),
            event.get("Victory"), 
            event.get("Score")
        ))
        print(f"Inserted StageEvent at {event.get('received_at')}")
    else:
        print(f"Skipped duplicate StageEvent at {event.get('received_at')}")
conn.commit()