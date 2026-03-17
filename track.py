"""
LinkedIn LegalTech Tracker — Wöchentlicher Brief
-------------------------------------------------
Scrapt LinkedIn-Posts zu LegalTech-Keywords und erstellt einen
wöchentlichen Kurzbericht in Airtable:
  - Welches Thema hat gerade das meiste Engagement?
  - Welche Hashtags sollst du nutzen?
  - Welche Posts haben diese Woche am besten performt?

Nutzung:
  python track.py              # Scrape + Airtable-Upload
  python track.py --dry-run    # Scrape ohne Upload (nur Konsole)
"""

import argparse
import json
import os
import re
import statistics
import time
from collections import Counter
from datetime import datetime, date

import requests
from apify_client import ApifyClient
from dotenv import load_dotenv

load_dotenv()

# --- Config ---

APIFY_TOKEN = os.getenv("APIFY_API_TOKEN")
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

KEYWORD_ACTOR = "apimaestro/linkedin-posts-search-scraper-no-cookies"
POSTS_PER_KEYWORD = 50

# Deutsche/DACH-fokussierte Keywords
KEYWORD_GROUPS = {
    "Legal Tech": ["legaltech", "legal tech deutschland"],
    "Legal AI": ["legal AI", "KI Recht", "KI Kanzlei"],
    "RAG / Rechtsrecherche": ["RAG legal", "KI Rechtsrecherche"],
    "AI Act / Compliance": ["AI Governance Recht", "AI Act", "compliance KI"],
    "Legal Automation": ["legal automation", "legal operations"],
    "Legal Innovation": ["legal innovation", "legal prompt engineering"],
}

OUTPUT_DIR = "output/weekly"


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


def extract_hashtags(text: str) -> list[str]:
    if not text:
        return []
    return [tag.lower() for tag in re.findall(r"#(\w+)", text)]


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

def scrape_all_keywords(client: ApifyClient) -> list[dict]:
    all_posts = []
    all_keywords = [
        (kw, group)
        for group, keywords in KEYWORD_GROUPS.items()
        for kw in keywords
    ]

    print(f"Scrape {len(all_keywords)} Keywords...\n")

    for i, (keyword, group) in enumerate(all_keywords, 1):
        print(f"[{i}/{len(all_keywords)}] '{keyword}' ({group})")
        try:
            run = client.actor(KEYWORD_ACTOR).call(
                run_input={
                    "keyword": keyword,
                    "sort_type": "date_posted",
                    "date_filter": "past-week",
                    "total_posts": POSTS_PER_KEYWORD,
                },
                timeout_secs=180,
            )
            items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
            for item in items:
                item["_keyword"] = keyword
                item["_group"] = group
            all_posts.extend(items)
            print(f"  -> {len(items)} Posts")
        except Exception as e:
            print(f"  -> FEHLER: {e}")

    # Deduplizieren
    seen = set()
    unique = []
    for post in all_posts:
        pid = (
            post.get("postUrl") or post.get("url")
            or hash(str(post.get("text", ""))[:200])
        )
        if pid not in seen:
            seen.add(pid)
            unique.append(post)

    print(f"\n{len(unique)} unique Posts (von {len(all_posts)} gesamt)")
    return unique


# --- Analyse ---

def parse_post(post: dict) -> dict:
    """Extrahiert die relevanten Felder aus einem Apify-Post."""
    author_data = post.get("author", {})
    if isinstance(author_data, dict):
        author = author_data.get("name", "")
    else:
        author = str(author_data) if author_data else ""

    stats = post.get("stats", {})
    if isinstance(stats, dict):
        likes = safe_int(stats.get("total_reactions") or stats.get("likes"))
        comments = safe_int(stats.get("comments"))
        shares = safe_int(stats.get("shares"))
    else:
        likes = comments = shares = 0
    engagement = likes + comments * 2 + shares * 3

    text = post.get("text") or post.get("content", "")
    if isinstance(text, dict):
        text = text.get("text", "")

    hashtags = post.get("hashtags", [])
    if isinstance(hashtags, list) and hashtags:
        tags = [t.lower().lstrip("#") for t in hashtags]
    else:
        tags = extract_hashtags(text)

    return {
        "author": author,
        "text": text[:300] if text else "",
        "likes": likes,
        "comments": comments,
        "shares": shares,
        "engagement": engagement,
        "hashtags": tags,
        "group": post.get("_group", ""),
        "url": post.get("postUrl") or post.get("url") or "",
    }


def build_weekly_brief(posts: list[dict], week: str) -> dict:
    """Baut den wöchentlichen Kurzbericht."""
    parsed = [parse_post(p) for p in posts]

    # --- Pro Themenfeld ---
    groups = {}
    for p in parsed:
        g = p["group"]
        if not g:
            continue
        if g not in groups:
            groups[g] = {"engagements": [], "hashtags": Counter(), "posts": []}
        groups[g]["engagements"].append(p["engagement"])
        groups[g]["posts"].append(p)
        for tag in p["hashtags"]:
            groups[g]["hashtags"][tag] += 1

    # Themenfeld-Ranking nach Engagement
    themen_ranking = []
    for g, data in sorted(groups.items(), key=lambda x: statistics.mean(x[1]["engagements"]), reverse=True):
        top_post = max(data["posts"], key=lambda x: x["engagement"])
        themen_ranking.append({
            "thema": g,
            "posts": len(data["engagements"]),
            "avg_engagement": round(statistics.mean(data["engagements"]), 1),
            "top_hashtags": [t for t, _ in data["hashtags"].most_common(5)],
            "best_post_text": top_post["text"][:150],
            "best_post_engagement": top_post["engagement"],
            "best_post_author": top_post["author"],
        })

    # --- Globale Top-Hashtags ---
    all_hashtags = Counter()
    for p in parsed:
        for tag in p["hashtags"]:
            all_hashtags[tag] += 1
    top_hashtags = [t for t, _ in all_hashtags.most_common(15)]

    # --- Top 5 Posts der Woche ---
    top_posts = sorted(parsed, key=lambda x: x["engagement"], reverse=True)[:5]

    # --- Empfohlene Hashtag-Kombination ---
    # Nimm die Top-3 global + Top-2 aus dem stärksten Themenfeld
    best_group_tags = themen_ranking[0]["top_hashtags"][:2] if themen_ranking else []
    empfohlene_hashtags = list(dict.fromkeys(top_hashtags[:3] + best_group_tags))[:5]

    return {
        "week": week,
        "total_posts": len(parsed),
        "themen_ranking": themen_ranking,
        "top_hashtags": top_hashtags,
        "empfohlene_hashtags": empfohlene_hashtags,
        "top_posts": top_posts,
    }


def brief_to_airtable(brief: dict) -> dict:
    """Wandelt den Brief in einen Airtable-Record für die Tabelle 'Wochenbriefe'."""
    ranking = brief["themen_ranking"]

    # Themen-Übersicht als lesbarer Text
    themen_text = ""
    for i, t in enumerate(ranking, 1):
        themen_text += (
            f"{i}. {t['thema']} — Ø {t['avg_engagement']} Engagement "
            f"({t['posts']} Posts)\n"
            f"   Top-Hashtags: #{', #'.join(t['top_hashtags'])}\n"
            f"   Bester Post ({t['best_post_author']}, {t['best_post_engagement']} Eng.): "
            f"\"{t['best_post_text']}...\"\n\n"
        )

    # Top Posts
    top_posts_text = ""
    for i, p in enumerate(brief["top_posts"], 1):
        top_posts_text += (
            f"{i}. {p['author']} ({p['engagement']} Eng.)\n"
            f"   \"{p['text'][:120]}...\"\n"
            f"   {p['url']}\n\n"
        )

    return {
        "woche": brief["week"],
        "datum": date.today().isoformat(),
        "posts_analysiert": brief["total_posts"],
        "heissestes_thema": ranking[0]["thema"] if ranking else "",
        "empfohlene_hashtags": ", ".join(f"#{t}" for t in brief["empfohlene_hashtags"]),
        "alle_top_hashtags": ", ".join(f"#{t}" for t in brief["top_hashtags"]),
        "themen_ranking": themen_text.strip(),
        "top_posts": top_posts_text.strip(),
    }


def print_brief(brief: dict):
    """Gibt den Brief auf der Konsole aus."""
    print(f"\n{'='*60}")
    print(f"  WOCHENBRIEF — {brief['week']}")
    print(f"  {brief['total_posts']} Posts analysiert")
    print(f"{'='*60}")

    print(f"\n--- EMPFOHLENE HASHTAGS ---")
    print(f"  {', '.join(f'#{t}' for t in brief['empfohlene_hashtags'])}")

    print(f"\n--- THEMEN-RANKING (nach Engagement) ---")
    for i, t in enumerate(brief["themen_ranking"], 1):
        print(f"\n  {i}. {t['thema']}")
        print(f"     Ø {t['avg_engagement']} Engagement ({t['posts']} Posts)")
        print(f"     Hashtags: {', '.join(f'#{h}' for h in t['top_hashtags'])}")
        print(f"     Best: \"{t['best_post_text'][:100]}...\"")
        print(f"           von {t['best_post_author']} ({t['best_post_engagement']} Eng.)")

    print(f"\n--- TOP 5 POSTS DER WOCHE ---")
    for i, p in enumerate(brief["top_posts"], 1):
        print(f"\n  {i}. {p['author']} — {p['engagement']} Engagement")
        print(f"     \"{p['text'][:120]}...\"")
        if p["url"]:
            print(f"     {p['url']}")

    print(f"\n{'='*60}\n")


# --- Main ---

def main():
    parser = argparse.ArgumentParser(description="LinkedIn LegalTech Wochenbrief")
    parser.add_argument("--dry-run", action="store_true", help="Kein Airtable-Upload")
    args = parser.parse_args()

    if not APIFY_TOKEN:
        print("FEHLER: APIFY_API_TOKEN nicht in .env!")
        return
    if not args.dry_run and (not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID):
        print("FEHLER: AIRTABLE_TOKEN und AIRTABLE_BASE_ID in .env setzen!")
        return

    week = get_week_label()
    print(f"=== LinkedIn LegalTech Tracker — {week} ===")
    print(f"Datum: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n")

    client = ApifyClient(APIFY_TOKEN)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Scrape
    posts = scrape_all_keywords(client)

    # 2. Brief erstellen
    brief = build_weekly_brief(posts, week)

    # 3. Lokal speichern
    local_file = os.path.join(OUTPUT_DIR, f"brief_{week}.json")
    with open(local_file, "w", encoding="utf-8") as f:
        json.dump(brief, f, ensure_ascii=False, indent=2, default=str)
    print(f"Lokale Sicherung: {local_file}")

    # 4. Ausgabe
    print_brief(brief)

    # 5. Airtable
    if not args.dry_run:
        record = brief_to_airtable(brief)
        created = airtable_create("Wochenbriefe", [record])
        if created:
            print(f"Wochenbrief in Airtable gespeichert.")
        else:
            print("FEHLER: Wochenbrief konnte nicht gespeichert werden.")


if __name__ == "__main__":
    main()
