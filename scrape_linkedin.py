"""
LinkedIn Legal Tech Post Scraper & Analyse
-------------------------------------------
Durchsucht LinkedIn nach Posts zu Legal Tech Themen (DACH-Raum),
identifiziert Top-Influencer, Hashtags und Content-Muster.

Nutzt Apify Actor: apimaestro/linkedin-posts-search-scraper-no-cookies
"""

import json
import os
import csv
import re
from collections import Counter
from datetime import datetime
from urllib.parse import quote_plus

from apify_client import ApifyClient
from dotenv import load_dotenv

load_dotenv()

# --- Konfiguration ---

ACTOR_ID = "apimaestro/linkedin-posts-search-scraper-no-cookies"

# Posts pro Keyword (max 50 pro Seite, auto-pagination holt mehr)
POSTS_PER_KEYWORD = 100

# Keyword-Strategie: Breite Abdeckung des deutschen Legal-Tech-Ökosystems
# Gruppiert nach Themenfeldern für spätere Analyse
KEYWORD_GROUPS = {
    "legal_tech_core": [
        "legaltech",
        "legal tech deutschland",
        "rechtstechnologie",
    ],
    "legal_ai": [
        "legal AI",
        "KI Recht",
        "künstliche Intelligenz Anwalt",
        "KI Kanzlei",
    ],
    "rag_legal": [
        "RAG legal",
        "legal RAG",
        "KI Rechtsrecherche",
    ],
    "governance_compliance": [
        "AI Governance Recht",
        "legal governance",
        "compliance KI",
        "AI Act",
    ],
    "legal_automation": [
        "legal automation",
        "Vertragsautomatisierung",
        "legal operations",
        "contract AI",
    ],
    "legal_innovation": [
        "legal design",
        "legal prompt engineering",
        "legal innovation",
        "RegTech",
    ],
}

OUTPUT_DIR = "output"
RESULTS_JSON = os.path.join(OUTPUT_DIR, "posts_raw.json")
RESULTS_CSV = os.path.join(OUTPUT_DIR, "posts.csv")
ANALYSIS_FILE = os.path.join(OUTPUT_DIR, "analyse.md")


def scrape_keyword(client: ApifyClient, keyword: str, group: str) -> list[dict]:
    """Führt einen Apify-Scrape für ein einzelnes Keyword durch."""
    print(f"  -> Scrape: '{keyword}' ({group})")

    run_input = {
        "keyword": keyword,
        "sort_type": "date_posted",
        "date_filter": "past-month",
        "total_posts": POSTS_PER_KEYWORD,
    }

    try:
        run = client.actor(ACTOR_ID).call(
            run_input=run_input,
            timeout_secs=180,
        )

        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())

        # Keyword-Gruppe und Suchbegriff an jeden Post anhängen
        for item in items:
            item["_keyword"] = keyword
            item["_group"] = group

        print(f"     -> {len(items)} Posts gefunden")
        return items

    except Exception as e:
        print(f"     -> FEHLER bei '{keyword}': {e}")
        return []


def deduplicate_posts(posts: list[dict]) -> list[dict]:
    """Entfernt doppelte Posts basierend auf Post-URL oder Text-Hash."""
    seen = set()
    unique = []
    for post in posts:
        # Versuche verschiedene ID-Felder
        post_id = (
            post.get("postUrl")
            or post.get("url")
            or post.get("link")
            or post.get("postId")
            or hash(post.get("text", "")[:200])
        )
        if post_id not in seen:
            seen.add(post_id)
            unique.append(post)
    return unique


def extract_hashtags(text: str) -> list[str]:
    """Extrahiert Hashtags aus Post-Text."""
    if not text:
        return []
    return [tag.lower() for tag in re.findall(r"#(\w+)", text)]


def safe_int(val) -> int:
    """Konvertiert einen Wert sicher zu int."""
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        # z.B. "1,234" oder "1.234" oder "12K"
        val = val.strip().replace(",", "").replace(".", "")
        if val.lower().endswith("k"):
            return int(float(val[:-1]) * 1000)
        try:
            return int(val)
        except ValueError:
            return 0
    return 0


def analyze_posts(posts: list[dict]) -> dict:
    """Analysiert die gesammelten Posts und erstellt Statistiken."""
    if not posts:
        return {
            "total_posts": 0,
            "unique_authors": 0,
            "top_authors_by_posts": [],
            "top_influencer_by_engagement": [],
            "top_hashtags": [],
            "posts_per_keyword": [],
            "posts_per_group": [],
        }

    # Autoren-Statistik
    authors = Counter()
    author_engagement = {}
    author_profiles = {}
    hashtags = Counter()
    keyword_counts = Counter()
    group_counts = Counter()

    for post in posts:
        # Autor identifizieren - Datenstruktur: author.name
        author_data = post.get("author", {})
        if isinstance(author_data, dict):
            author = author_data.get("name", "Unbekannt")
            profile_url = author_data.get("profile_url", "")
        else:
            author = str(author_data) if author_data else "Unbekannt"
            profile_url = ""
        authors[author] += 1

        # Profil-URL merken
        if profile_url and author not in author_profiles:
            author_profiles[author] = profile_url

        # Engagement berechnen - Datenstruktur: stats.total_reactions / comments / shares
        stats = post.get("stats", {})
        if isinstance(stats, dict):
            likes = safe_int(stats.get("total_reactions"))
            comments = safe_int(stats.get("comments"))
            shares = safe_int(stats.get("shares"))
        else:
            likes = comments = shares = 0
        engagement = likes + comments * 2 + shares * 3  # Gewichtung

        if author not in author_engagement:
            author_engagement[author] = {"total_engagement": 0, "posts": 0}
        author_engagement[author]["total_engagement"] += engagement
        author_engagement[author]["posts"] += 1

        # Hashtags - direkt als Liste im Post verfügbar
        post_hashtags = post.get("hashtags", [])
        if isinstance(post_hashtags, list):
            for tag in post_hashtags:
                hashtags[tag.lower().lstrip("#")] += 1
        # Auch aus Text extrahieren als Fallback
        text = post.get("text", "")
        if not post_hashtags and text:
            for tag in extract_hashtags(text):
                hashtags[tag] += 1

        # Keywords & Gruppen
        keyword_counts[post.get("_keyword", "?")] += 1
        group_counts[post.get("_group", "?")] += 1

    # Top-Influencer nach Engagement
    top_influencer = sorted(
        author_engagement.items(),
        key=lambda x: x[1]["total_engagement"],
        reverse=True,
    )[:30]

    return {
        "total_posts": len(posts),
        "unique_authors": len(authors),
        "top_authors_by_posts": authors.most_common(30),
        "top_influencer_by_engagement": [
            {
                "name": name,
                "profile_url": author_profiles.get(name, ""),
                "posts": data["posts"],
                "total_engagement": data["total_engagement"],
                "avg_engagement": round(data["total_engagement"] / data["posts"], 1),
            }
            for name, data in top_influencer
        ],
        "top_hashtags": hashtags.most_common(40),
        "posts_per_keyword": keyword_counts.most_common(),
        "posts_per_group": group_counts.most_common(),
        "author_profiles": author_profiles,
    }


def write_analysis_report(analysis: dict, filepath: str):
    """Schreibt einen Markdown-Analysebericht."""
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("# LinkedIn Legal Tech Analyse\n\n")
        f.write(f"*Erstellt am {datetime.now().strftime('%d.%m.%Y %H:%M')}*\n\n")

        f.write("## Überblick\n\n")
        f.write(f"- **Gesamte Posts:** {analysis['total_posts']}\n")
        f.write(f"- **Einzigartige Autoren:** {analysis['unique_authors']}\n\n")

        # Top Influencer
        f.write("## Top 30 Influencer (nach Engagement)\n\n")
        f.write("| # | Name | Profil | Posts | Gesamt-Engagement | Ø Engagement |\n")
        f.write("|---|------|--------|-------|-------------------|---------------|\n")
        for i, inf in enumerate(analysis["top_influencer_by_engagement"], 1):
            profile = f"[Profil]({inf['profile_url']})" if inf["profile_url"] else "-"
            f.write(
                f"| {i} | {inf['name']} | {profile} | {inf['posts']} | "
                f"{inf['total_engagement']} | {inf['avg_engagement']} |\n"
            )

        # Top Hashtags
        f.write("\n## Top 40 Hashtags\n\n")
        f.write("| # | Hashtag | Anzahl |\n")
        f.write("|---|---------|--------|\n")
        for i, (tag, count) in enumerate(analysis["top_hashtags"], 1):
            f.write(f"| {i} | #{tag} | {count} |\n")

        # Posts pro Themengruppe
        f.write("\n## Posts pro Themenfeld\n\n")
        f.write("| Themenfeld | Posts |\n")
        f.write("|------------|-------|\n")
        for group, count in analysis["posts_per_group"]:
            f.write(f"| {group} | {count} |\n")

        # Posts pro Keyword
        f.write("\n## Posts pro Keyword\n\n")
        f.write("| Keyword | Posts |\n")
        f.write("|---------|-------|\n")
        for kw, count in analysis["posts_per_keyword"]:
            f.write(f"| {kw} | {count} |\n")

        # Aktivste Poster
        f.write("\n## Top 30 Autoren (nach Anzahl Posts)\n\n")
        f.write("| # | Name | Posts |\n")
        f.write("|---|------|-------|\n")
        for i, (name, count) in enumerate(analysis["top_authors_by_posts"], 1):
            f.write(f"| {i} | {name} | {count} |\n")

    print(f"\nAnalyse-Report geschrieben: {filepath}")


def export_csv(posts: list[dict], filepath: str):
    """Exportiert Posts als CSV."""
    if not posts:
        print("Keine Posts zum Exportieren.")
        return

    # Alle Felder aus den Daten sammeln
    all_fields = set()
    for post in posts:
        all_fields.update(post.keys())

    # Wichtige Felder zuerst
    priority_fields = [
        "_keyword", "_group",
        "authorName", "author", "profileName", "authorFullName", "name",
        "authorProfileUrl", "profileUrl",
        "text", "postText", "content",
        "likes", "numLikes", "totalReactionCount",
        "comments", "numComments", "commentsCount",
        "shares", "numShares", "repostCount",
        "postUrl", "url", "link",
        "date", "postedAt", "timestamp", "postedDate",
    ]
    fields = [f for f in priority_fields if f in all_fields]
    for f in sorted(all_fields):
        if f not in fields:
            fields.append(f)

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for post in posts:
            # Texte kürzen für CSV-Lesbarkeit
            row = {}
            for k, v in post.items():
                if isinstance(v, (list, dict)):
                    row[k] = json.dumps(v, ensure_ascii=False)
                else:
                    row[k] = v
            writer.writerow(row)

    print(f"CSV exportiert: {filepath} ({len(posts)} Zeilen)")


def main():
    token = os.getenv("APIFY_API_TOKEN")
    if not token:
        print("FEHLER: APIFY_API_TOKEN nicht in .env gefunden!")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    client = ApifyClient(token)
    all_posts = []

    # Alle Keywords durcharbeiten
    all_keywords = [
        (kw, group)
        for group, keywords in KEYWORD_GROUPS.items()
        for kw in keywords
    ]

    print(f"Starte LinkedIn-Scraping mit {len(all_keywords)} Keywords...")
    print(f"Actor: {ACTOR_ID}")
    print(f"Posts pro Keyword: {POSTS_PER_KEYWORD}")
    print(f"Datumsfilter: letzter Monat\n")

    for i, (keyword, group) in enumerate(all_keywords, 1):
        print(f"[{i}/{len(all_keywords)}] Gruppe: {group}")
        posts = scrape_keyword(client, keyword, group)
        all_posts.extend(posts)
        print()

    # Deduplizieren
    print(f"Gesamt gesammelt: {len(all_posts)} Posts")
    all_posts = deduplicate_posts(all_posts)
    print(f"Nach Deduplizierung: {len(all_posts)} Posts\n")

    # Rohdaten speichern
    with open(RESULTS_JSON, "w", encoding="utf-8") as f:
        json.dump(all_posts, f, ensure_ascii=False, indent=2, default=str)
    print(f"Rohdaten gespeichert: {RESULTS_JSON}")

    # CSV exportieren
    export_csv(all_posts, RESULTS_CSV)

    # Analyse durchführen
    print("\nAnalyse läuft...")
    analysis = analyze_posts(all_posts)

    # Analyse als JSON
    analysis_json = os.path.join(OUTPUT_DIR, "analyse.json")
    with open(analysis_json, "w", encoding="utf-8") as f:
        json.dump(analysis, f, ensure_ascii=False, indent=2, default=str)

    # Analyse-Report als Markdown
    write_analysis_report(analysis, ANALYSIS_FILE)

    print("\n--- ZUSAMMENFASSUNG ---")
    print(f"Posts gesamt:        {analysis['total_posts']}")
    print(f"Einzigartige Autoren: {analysis['unique_authors']}")
    if analysis["top_hashtags"]:
        print(f"\nTop 10 Hashtags:")
        for tag, count in analysis["top_hashtags"][:10]:
            print(f"  #{tag}: {count}")
    if analysis["top_influencer_by_engagement"]:
        print(f"\nTop 10 Influencer:")
        for inf in analysis["top_influencer_by_engagement"][:10]:
            print(f"  {inf['name']}: {inf['total_engagement']} Engagement ({inf['posts']} Posts)")

    print(f"\nAlle Ergebnisse in: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
