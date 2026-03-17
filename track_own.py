"""
Eigene LinkedIn-Profil Performance — Wöchentliches Tracking
------------------------------------------------------------
Scrapt die eigenen LinkedIn-Posts, berechnet Engagement-Deltas
gegenüber der Vorwoche und speichert die Ergebnisse lokal + Airtable.

Nutzung:
  python track_own.py              # Scrape + Airtable-Upload
  python track_own.py --dry-run    # Scrape ohne Upload (nur Konsole)
"""

import argparse
import json
import os
import re
import time
from datetime import datetime, date

import requests
from apify_client import ApifyClient
from dotenv import load_dotenv

load_dotenv()

# --- Config ---

APIFY_TOKEN = os.getenv("APIFY_API_TOKEN")
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

OWN_PROFILE_URL = os.getenv("LINKEDIN_PROFILE_URL", "https://www.linkedin.com/in/jonaskarioui/")

ACTOR_ID = "harvestapi/linkedin-profile-posts"
MAX_POSTS = 50

OUTPUT_DIR = "output/weekly"
AIRTABLE_TABLE = "Eigene Posts"


# --- Helpers ---

def safe_int(val) -> int:
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        val = val.strip().replace(",", "").replace(".", "")
        if val.lower().endswith("k"):
            return int(float(val[:-1]) * 1000)
        try:
            return int(val)
        except ValueError:
            return 0
    return 0


def get_week_label() -> str:
    today = date.today()
    return f"{today.isocalendar()[0]}-W{today.isocalendar()[1]:02d}"


def airtable_headers():
    return {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }


def airtable_url(table_name: str) -> str:
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{requests.utils.quote(table_name)}"


def airtable_create(table_name: str, records: list[dict]) -> int:
    url = airtable_url(table_name)
    created = 0
    for i in range(0, len(records), 10):
        batch = records[i:i + 10]
        payload = {"records": [{"fields": r} for r in batch]}
        resp = requests.post(url, json=payload, headers=airtable_headers())
        if resp.status_code == 200:
            created += len(batch)
        elif resp.status_code == 429:
            time.sleep(30)
            resp = requests.post(url, json=payload, headers=airtable_headers())
            if resp.status_code == 200:
                created += len(batch)
        else:
            print(f"  FEHLER [{table_name}]: {resp.status_code} — {resp.text[:200]}")
        if i + 10 < len(records):
            time.sleep(0.2)
    return created


# --- Scraping ---

def scrape_own_posts(client: ApifyClient) -> list[dict]:
    """Scrapt die eigenen LinkedIn-Posts via Apify."""
    print(f"Scrape eigene Posts: {OWN_PROFILE_URL}")

    run_input = {
        "targetUrls": [OWN_PROFILE_URL],
        "postedLimit": "3months",
        "maxPosts": MAX_POSTS,
        "includeQuotePosts": True,
        "includeReposts": False,
        "scrapeReactions": False,
    }

    try:
        run = client.actor(ACTOR_ID).call(
            run_input=run_input,
            timeout_secs=300,
        )
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        print(f"-> {len(items)} Posts gesammelt\n")
        return items
    except Exception as e:
        print(f"FEHLER beim Scraping: {e}")
        return []


# --- Parsing ---

def parse_post(post: dict) -> dict:
    """Extrahiert die relevanten Felder aus einem Apify-Post."""
    # Engagement — Datenstruktur des harvestapi-Actors: engagement.likes / comments / shares
    eng_data = post.get("engagement", {})
    if isinstance(eng_data, dict):
        likes = safe_int(eng_data.get("likes"))
        comments = safe_int(eng_data.get("comments"))
        shares = safe_int(eng_data.get("shares"))
    else:
        stats = post.get("stats", {})
        likes = safe_int(stats.get("total_reactions") or stats.get("likes") or post.get("likes"))
        comments = safe_int(stats.get("comments") or post.get("comments"))
        shares = safe_int(stats.get("shares") or post.get("shares"))

    engagement = likes + comments * 2 + shares * 3

    # Text
    text = post.get("content", "")
    if isinstance(text, dict):
        text = text.get("text", "")

    # URL
    url = post.get("linkedinUrl") or post.get("post_url") or post.get("postUrl") or ""

    # Datum
    post_date = post.get("postedAt", "")

    return {
        "post_text": text[:100] if text else "",
        "full_text": text[:300] if text else "",
        "likes": likes,
        "comments": comments,
        "shares": shares,
        "engagement": engagement,
        "datum": post_date,
        "url": url,
    }


# --- Delta-Berechnung ---

def load_previous_data() -> list[dict]:
    """Lädt die vorherige own_posts.json, falls vorhanden."""
    filepath = os.path.join(OUTPUT_DIR, "own_posts.json")
    if not os.path.exists(filepath):
        return []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("posts", [])
    except (json.JSONDecodeError, KeyError):
        return []


def compute_deltas(current: list[dict], previous: list[dict]) -> list[dict]:
    """Berechnet Deltas (neue Likes, Kommentare etc. seit letztem Check)."""
    # Index vorherige Posts nach URL
    prev_by_url = {}
    for p in previous:
        if p.get("url"):
            prev_by_url[p["url"]] = p

    for post in current:
        url = post.get("url", "")
        prev = prev_by_url.get(url)
        if prev:
            post["delta_likes"] = post["likes"] - prev.get("likes", 0)
            post["delta_comments"] = post["comments"] - prev.get("comments", 0)
            post["delta_shares"] = post["shares"] - prev.get("shares", 0)
            post["delta_engagement"] = post["engagement"] - prev.get("engagement", 0)
            post["is_new"] = False
        else:
            # Neuer Post seit letztem Check
            post["delta_likes"] = post["likes"]
            post["delta_comments"] = post["comments"]
            post["delta_shares"] = post["shares"]
            post["delta_engagement"] = post["engagement"]
            post["is_new"] = True

    return current


# --- Ausgabe ---

def print_own_report(posts: list[dict], week: str):
    """Gibt die eigene Performance auf der Konsole aus."""
    print(f"\n{'='*60}")
    print(f"  EIGENE POSTS — {week}")
    print(f"  {len(posts)} Posts analysiert")
    print(f"{'='*60}")

    total_eng = sum(p["engagement"] for p in posts)
    total_delta = sum(p.get("delta_engagement", 0) for p in posts)
    print(f"\n  Gesamt-Engagement: {total_eng} (Delta: {total_delta:+d})")

    sorted_posts = sorted(posts, key=lambda x: x["engagement"], reverse=True)

    print(f"\n--- POSTS NACH ENGAGEMENT ---")
    for i, p in enumerate(sorted_posts, 1):
        new_marker = " [NEU]" if p.get("is_new") else ""
        delta_str = f" (Delta: {p.get('delta_engagement', 0):+d})" if not p.get("is_new") else ""
        print(f"\n  {i}. [{p['engagement']} Eng.]{delta_str}{new_marker}")
        print(f"     {p['likes']}L / {p['comments']}C / {p['shares']}S")
        print(f"     \"{p['post_text']}...\"")
        if p.get("datum"):
            print(f"     Datum: {p['datum']}")
        if p.get("url"):
            print(f"     {p['url']}")

    print(f"\n{'='*60}\n")


# --- Airtable ---

def posts_to_airtable_records(posts: list[dict], week: str) -> list[dict]:
    """Wandelt Posts in Airtable-Records für die Tabelle 'Eigene Posts'."""
    records = []
    for p in posts:
        # Datum konvertieren (Airtable erwartet ISO-Format)
        datum = p.get("datum", "")
        if datum:
            try:
                # Versuche verschiedene Formate
                for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"):
                    try:
                        parsed = datetime.strptime(datum[:len(fmt.replace("%", "X"))], fmt)
                        datum = parsed.strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        continue
                else:
                    # Falls kein Format passt, Datum als String bis 10 Zeichen
                    datum = datum[:10] if len(datum) >= 10 else datum
            except Exception:
                datum = ""

        records.append({
            "post_text": p["post_text"],
            "datum": datum if datum else None,
            "likes": p["likes"],
            "comments": p["comments"],
            "shares": p["shares"],
            "engagement": p["engagement"],
            "delta_engagement": p.get("delta_engagement", 0),
            "url": p.get("url", ""),
            "woche": week,
        })
    # Airtable-Records ohne None-Datum-Felder
    for r in records:
        if r["datum"] is None:
            del r["datum"]
    return records


# --- Main ---

def main():
    parser = argparse.ArgumentParser(description="Eigene LinkedIn Performance tracken")
    parser.add_argument("--dry-run", action="store_true", help="Kein Airtable-Upload")
    args = parser.parse_args()

    if not APIFY_TOKEN:
        print("FEHLER: APIFY_API_TOKEN nicht in .env!")
        return
    if not args.dry_run and (not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID):
        print("FEHLER: AIRTABLE_TOKEN und AIRTABLE_BASE_ID in .env setzen!")
        return

    week = get_week_label()
    print(f"=== Eigene LinkedIn Performance — {week} ===")
    print(f"Datum: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n")

    client = ApifyClient(APIFY_TOKEN)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Scrape
    raw_posts = scrape_own_posts(client)
    if not raw_posts:
        print("Keine Posts erhalten. Abbruch.")
        return

    # 2. Parsen
    parsed = [parse_post(p) for p in raw_posts]

    # 3. Deltas berechnen
    previous = load_previous_data()
    parsed = compute_deltas(parsed, previous)

    # 4. Lokal speichern — aktuelle Daten (Basis für nächsten Delta-Vergleich)
    current_file = os.path.join(OUTPUT_DIR, "own_posts.json")
    with open(current_file, "w", encoding="utf-8") as f:
        json.dump({"week": week, "posts": parsed}, f, ensure_ascii=False, indent=2, default=str)
    print(f"Aktuelle Daten: {current_file}")

    # 5. Wochendatei speichern
    week_file = os.path.join(OUTPUT_DIR, f"own_posts_{week}.json")
    with open(week_file, "w", encoding="utf-8") as f:
        json.dump({"week": week, "posts": parsed}, f, ensure_ascii=False, indent=2, default=str)
    print(f"Wochensicherung: {week_file}")

    # 6. Konsolen-Report
    print_own_report(parsed, week)

    # 7. Airtable
    if not args.dry_run:
        records = posts_to_airtable_records(parsed, week)
        created = airtable_create(AIRTABLE_TABLE, records)
        if created:
            print(f"{created} Posts in Airtable-Tabelle '{AIRTABLE_TABLE}' gespeichert.")
        else:
            print(f"FEHLER: Posts konnten nicht in '{AIRTABLE_TABLE}' gespeichert werden.")


if __name__ == "__main__":
    main()
