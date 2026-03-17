"""
LinkedIn Influencer-Profil-Scraper
----------------------------------
Scrapt Posts von bekannten Legal-Tech-Influencern,
analysiert deren Content-Muster, Hashtags und Interaktionen.

Nutzt Apify Actor: harvestapi/linkedin-profile-posts
"""

import json
import os
import re
from collections import Counter
from datetime import datetime

from apify_client import ApifyClient
from dotenv import load_dotenv

load_dotenv()

ACTOR_ID = "harvestapi/linkedin-profile-posts"
POSTS_PER_PROFILE = 50

# Influencer-Profile und Posts zum Analysieren
TARGETS = [
    {
        "url": "https://www.linkedin.com/in/braegel/",
        "name": "Tom Braegelmann",
    },
    {
        "url": "https://www.linkedin.com/in/chan-jo-jun-9381022/",
        "name": "Chan-jo Jun",
    },
    {
        "url": "https://www.linkedin.com/in/leif-nissen-lundbæk-phd-a00a32141/",
        "name": "Leif Nissen Lundbæk",
    },
    {
        "url": "https://www.linkedin.com/in/alexandersporenberg/",
        "name": "Alexander Sporenberg",
    },
    {
        "url": "https://www.linkedin.com/feed/update/urn:li:activity:7432344496270516225/",
        "name": "Einzelpost (Referenz)",
    },
]

OUTPUT_DIR = "output"


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


def scrape_profiles(client: ApifyClient) -> list[dict]:
    """Scrapt alle Profil-Posts in einem einzigen Actor-Run."""
    urls = [t["url"] for t in TARGETS]
    print(f"Scrape {len(urls)} Targets...")
    for t in TARGETS:
        print(f"  - {t['name']}: {t['url']}")

    run_input = {
        "targetUrls": urls,
        "postedLimit": "3months",
        "maxPosts": POSTS_PER_PROFILE,
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
        print(f"\n-> {len(items)} Posts gesammelt\n")
        return items
    except Exception as e:
        print(f"FEHLER: {e}")
        return []


def analyze_influencer_posts(posts: list[dict]):
    """Analysiert Posts der Influencer im Detail."""
    # Posts nach Autor gruppieren
    by_author = {}
    all_hashtags = Counter()
    all_mentioned = Counter()

    for post in posts:
        # Autor bestimmen - Datenstruktur: author.name
        author_data = post.get("author", {})
        if isinstance(author_data, dict):
            author = author_data.get("name", "Unbekannt")
            profile_url = author_data.get("linkedinUrl", "")
        else:
            author = str(author_data) if author_data else "Unbekannt"
            profile_url = ""

        if author not in by_author:
            by_author[author] = {
                "posts": [],
                "hashtags": Counter(),
                "total_engagement": 0,
                "mentioned_people": Counter(),
                "profile_url": profile_url,
            }

        # Engagement - Datenstruktur: engagement.likes / comments / shares
        eng_data = post.get("engagement", {})
        if isinstance(eng_data, dict):
            likes = safe_int(eng_data.get("likes"))
            comments = safe_int(eng_data.get("comments"))
            shares = safe_int(eng_data.get("shares"))
        else:
            # Fallback für andere Datenformate
            stats = post.get("stats", {})
            likes = safe_int(stats.get("total_reactions") or stats.get("likes") or post.get("likes"))
            comments = safe_int(stats.get("comments") or post.get("comments"))
            shares = safe_int(stats.get("shares") or post.get("shares"))
        engagement = likes + comments * 2 + shares * 3

        # Text - Datenstruktur: content (string)
        text = post.get("content", "")
        if isinstance(text, dict):
            text = text.get("text", "")

        # Hashtags aus Text extrahieren
        for tag in extract_hashtags(text):
            by_author[author]["hashtags"][tag] += 1
            all_hashtags[tag] += 1

        # Erwähnte Personen extrahieren (contentAttributes mit PROFILE_MENTION)
        for attr in post.get("contentAttributes", []):
            if attr.get("type") == "PROFILE_MENTION":
                profile = attr.get("profile", {})
                mention_name = f"{profile.get('firstName', '')} {profile.get('lastName', '')}".strip()
                mention_url = profile.get("linkedinUrl", "")
                if mention_name:
                    by_author[author]["mentioned_people"][mention_name] += 1
                    all_mentioned[mention_name] += 1

        by_author[author]["total_engagement"] += engagement
        by_author[author]["posts"].append({
            "text": text[:300] if text else "",
            "likes": likes,
            "comments": comments,
            "shares": shares,
            "engagement": engagement,
            "url": post.get("linkedinUrl") or post.get("post_url") or post.get("postUrl") or "",
            "date": post.get("postedAt", ""),
        })

    return by_author, all_hashtags, all_mentioned


def write_influencer_report(by_author: dict, all_hashtags: Counter, all_mentioned: Counter, filepath: str):
    """Schreibt den Influencer-Analyse-Report."""
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("# LinkedIn Influencer-Analyse: Legal Tech\n\n")
        f.write(f"*Erstellt am {datetime.now().strftime('%d.%m.%Y %H:%M')}*\n\n")

        # Überblick
        total_posts = sum(len(d["posts"]) for d in by_author.values())
        f.write("## Überblick\n\n")
        f.write(f"- **Analysierte Profile:** {len(by_author)}\n")
        f.write(f"- **Gesamte Posts:** {total_posts}\n\n")

        # Pro Influencer
        for author, data in sorted(by_author.items(), key=lambda x: x[1]["total_engagement"], reverse=True):
            posts = data["posts"]
            avg_eng = round(data["total_engagement"] / len(posts), 1) if posts else 0

            f.write(f"---\n\n## {author}\n\n")
            if data.get("profile_url"):
                f.write(f"[LinkedIn-Profil]({data['profile_url']})\n\n")
            f.write(f"- **Posts analysiert:** {len(posts)}\n")
            f.write(f"- **Gesamt-Engagement:** {data['total_engagement']}\n")
            f.write(f"- **Ø Engagement/Post:** {avg_eng}\n\n")

            # Top Hashtags dieses Autors
            if data["hashtags"]:
                f.write("### Meistgenutzte Hashtags\n\n")
                for tag, count in data["hashtags"].most_common(15):
                    f.write(f"- #{tag} ({count}x)\n")
                f.write("\n")

            # Erwähnte Personen (Netzwerk)
            if data.get("mentioned_people"):
                f.write("### Netzwerk (erwähnte Personen)\n\n")
                for name, count in data["mentioned_people"].most_common(15):
                    f.write(f"- {name} ({count}x erwähnt)\n")
                f.write("\n")

            # Top 5 Posts nach Engagement
            top_posts = sorted(posts, key=lambda p: p["engagement"], reverse=True)[:5]
            if top_posts:
                f.write("### Top-Posts (nach Engagement)\n\n")
                for i, p in enumerate(top_posts, 1):
                    f.write(f"**{i}. [{p['engagement']} Engagement]** ")
                    f.write(f"({p['likes']} Likes, {p['comments']} Kommentare, {p['shares']} Shares)\n")
                    if p["date"]:
                        f.write(f"*{p['date']}*\n")
                    if p["url"]:
                        f.write(f"[Link]({p['url']})\n")
                    text_preview = p["text"].replace("\n", " ")[:200]
                    f.write(f"> {text_preview}...\n\n")

        # Gemeinsame Hashtags
        f.write("---\n\n## Gemeinsame Hashtag-Analyse (alle Influencer)\n\n")
        f.write("| # | Hashtag | Häufigkeit |\n")
        f.write("|---|---------|------------|\n")
        for i, (tag, count) in enumerate(all_hashtags.most_common(30), 1):
            f.write(f"| {i} | #{tag} | {count} |\n")

        # Netzwerk-Map: Am häufigsten erwähnte Personen
        if all_mentioned:
            f.write("\n## Netzwerk-Map: Meisterwähnte Personen\n\n")
            f.write("Diese Personen werden von den Influencern am häufigsten getaggt – potenzielle Kontakte:\n\n")
            f.write("| # | Name | Erwähnungen |\n")
            f.write("|---|------|-------------|\n")
            for i, (name, count) in enumerate(all_mentioned.most_common(30), 1):
                f.write(f"| {i} | {name} | {count} |\n")

        # Hashtag-Empfehlungen
        f.write("\n## Empfohlene Hashtags für deine Kampagne\n\n")
        f.write("Basierend auf der Influencer-Analyse und der Keyword-Recherche:\n\n")

        f.write("### Must-Use (hohe Reichweite)\n")
        high_freq = [tag for tag, c in all_hashtags.most_common(10)]
        for tag in high_freq:
            f.write(f"- #{tag}\n")

        f.write("\n### Nischen-Hashtags (weniger Konkurrenz, gezielter)\n")
        niche = [tag for tag, c in all_hashtags.most_common(30)[10:20]]
        for tag in niche:
            f.write(f"- #{tag}\n")

    print(f"Report geschrieben: {filepath}")


def main():
    token = os.getenv("APIFY_API_TOKEN")
    if not token:
        print("FEHLER: APIFY_API_TOKEN nicht in .env gefunden!")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    client = ApifyClient(token)

    # Scrape
    posts = scrape_profiles(client)
    if not posts:
        print("Keine Posts erhalten. Abbruch.")
        return

    # Rohdaten speichern
    raw_file = os.path.join(OUTPUT_DIR, "influencer_posts_raw.json")
    with open(raw_file, "w", encoding="utf-8") as f:
        json.dump(posts, f, ensure_ascii=False, indent=2, default=str)
    print(f"Rohdaten: {raw_file}")

    # Analysieren
    by_author, all_hashtags, all_mentioned = analyze_influencer_posts(posts)

    # Report
    report_file = os.path.join(OUTPUT_DIR, "influencer_analyse.md")
    write_influencer_report(by_author, all_hashtags, all_mentioned, report_file)

    # JSON-Analyse
    analysis_file = os.path.join(OUTPUT_DIR, "influencer_analyse.json")
    analysis = {
        "authors": {
            name: {
                "post_count": len(data["posts"]),
                "total_engagement": data["total_engagement"],
                "avg_engagement": round(data["total_engagement"] / len(data["posts"]), 1) if data["posts"] else 0,
                "top_hashtags": data["hashtags"].most_common(15),
            }
            for name, data in by_author.items()
        },
        "combined_hashtags": all_hashtags.most_common(30),
    }
    with open(analysis_file, "w", encoding="utf-8") as f:
        json.dump(analysis, f, ensure_ascii=False, indent=2, default=str)

    # Summary
    print("\n--- INFLUENCER-ZUSAMMENFASSUNG ---")
    for author, data in sorted(by_author.items(), key=lambda x: x[1]["total_engagement"], reverse=True):
        avg = round(data["total_engagement"] / len(data["posts"]), 1) if data["posts"] else 0
        tags = ", ".join(f"#{t}" for t, _ in data["hashtags"].most_common(5))
        print(f"\n{author}:")
        print(f"  {len(data['posts'])} Posts | Ø {avg} Engagement")
        print(f"  Hashtags: {tags}")


if __name__ == "__main__":
    main()
