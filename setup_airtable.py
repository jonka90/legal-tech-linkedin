"""
Airtable-Setup für LinkedIn Tracking
-------------------------------------
Erstellt die Tabelle 'Wochenbriefe' in der bestehenden Base.
Einmalig ausführen: python setup_airtable.py
"""

import os
import sys
import requests
from dotenv import load_dotenv

load_dotenv()

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
API_URL = "https://api.airtable.com/v0/meta/bases"


def headers():
    return {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }


def create_table(name: str, fields: list[dict], description: str = "") -> dict:
    url = f"{API_URL}/{AIRTABLE_BASE_ID}/tables"
    payload = {"name": name, "description": description, "fields": fields}
    resp = requests.post(url, json=payload, headers=headers())
    if resp.status_code == 200:
        print(f"  Tabelle '{name}' erstellt.")
        return resp.json()
    elif "DUPLICATE_TABLE_NAME" in resp.text:
        print(f"  Tabelle '{name}' existiert bereits.")
        return {}
    else:
        print(f"  FEHLER bei '{name}': {resp.status_code} — {resp.text}")
        return {}


def main():
    if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID:
        print("FEHLER: AIRTABLE_TOKEN und AIRTABLE_BASE_ID in .env setzen!")
        sys.exit(1)

    print(f"Setup in Base {AIRTABLE_BASE_ID}...\n")

    create_table(
        name="Wochenbriefe",
        description="Wöchentlicher Kurzbericht: Trends, Hashtags, Top-Posts",
        fields=[
            {"name": "woche", "type": "singleLineText", "description": "z.B. 2026-W12"},
            {"name": "datum", "type": "date", "options": {"dateFormat": {"name": "iso"}}},
            {"name": "posts_analysiert", "type": "number", "options": {"precision": 0}},
            {"name": "heissestes_thema", "type": "singleLineText"},
            {"name": "empfohlene_hashtags", "type": "singleLineText",
             "description": "Die 5 Hashtags die du diese Woche nutzen solltest"},
            {"name": "alle_top_hashtags", "type": "singleLineText",
             "description": "Top 15 Hashtags der Woche"},
            {"name": "themen_ranking", "type": "multilineText",
             "description": "Alle Themenfelder sortiert nach Engagement"},
            {"name": "top_posts", "type": "multilineText",
             "description": "Die 5 besten Posts der Woche mit Links"},
        ],
    )

    print("\nFertig!")


if __name__ == "__main__":
    main()
