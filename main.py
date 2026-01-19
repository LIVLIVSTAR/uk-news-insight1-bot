import os
import re
import hashlib
import sqlite3
import asyncio
from datetime import datetime, timezone

import feedparser
from rapidfuzz import fuzz
from telegram import Bot
from telegram.error import RetryAfter


# ---------------- CONFIG ----------------
BREAKING_CHAT_ID = int(os.getenv("BREAKING_CHAT_ID", "-1003118967605"))
MACRO_CHAT_ID    = int(os.getenv("MACRO_CHAT_ID", "-1003153326236"))
SPORTS_CHAT_ID   = int(os.getenv("SPORTS_CHAT_ID", "-1001803566974"))

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN env var")

POLL_SECONDS  = int(os.getenv("POLL_SECONDS", "600"))   # safer default
SIM_THRESHOLD = int(os.getenv("SIM_THRESHOLD", "92"))   # stronger dedup

bot = Bot(token=BOT_TOKEN)

BREAKING_FEEDS = [
    "http://newsrss.bbc.co.uk/rss/newsonline_uk_edition/front_page/rss.xml",
    "http://newsrss.bbc.co.uk/rss/newsonline_uk_edition/uk/rss.xml",
    "http://newsrss.bbc.co.uk/rss/newsonline_uk_edition/world/rss.xml",
    "https://feeds.skynews.com/feeds/rss/home.xml",
    "https://feeds.skynews.com/feeds/rss/uk.xml",
    "https://feeds.skynews.com/feeds/rss/world.xml",
]

MACRO_FEEDS = [
    "http://newsrss.bbc.co.uk/rss/newsonline_uk_edition/business/rss.xml",
    "https://www.ons.gov.uk/ons/media-centre/rss.xml",
]

SPORTS_FEEDS = [
    "https://feeds.bbci.co.uk/sport/rss.xml",
]

CHANNEL_CHAT_ID = {
    "breaking": BREAKING_CHAT_ID,
    "macro": MACRO_CHAT_ID,
    "sports": SPORTS_CHAT_ID,
}

CHANNEL_HASHTAGS = {
    "breaking": ("#Breaking", "#UK"),
    "macro": ("#Macro", "#UK"),
    "sports": ("#Sports", "#UK"),
}

SPORTS_KW = [
    "goal", "injury", "transfer", "match", "premier league", "football", "coach", "manager"
]
MACRO_KW = [
    "inflation", "cpi", "ppi", "gdp", "rates", "interest rate", "bank of england", "ons",
    "unemployment", "wages", "gbp", "economy", "recession", "bond", "yield"
]


# ---------------- DB (persistent dedup) ----------------
DB_PATH = "state.db"

def db_init():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS posted (
        k TEXT PRIMARY KEY,
        norm TEXT,
        ts TEXT
      )
    """)
    con.commit()
    con.close()

def db_has_key(k: str) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM posted WHERE k=? LIMIT 1", (k,))
    row = cur.fetchone()
    con.close()
    return row is not None

def db_insert(k: str, norm: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO posted (k, norm, ts) VALUES (?, ?, ?)",
        (k, norm, datetime.now(timezone.utc).isoformat())
    )
    con.commit()
    con.close()

def db_recent_norms(limit: int = 400):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT norm FROM posted ORDER BY ts DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    con.close()
    return [r[0] for r in rows if r and r[0]]


# ---------------- HELPERS ----------------
def normalize(text: str) -> str:
    t = (text or "").lower()
    t = re.sub(r"[\W_]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def make_key(title: str, link: str) -> str:
    base = (normalize(title) + "|" + (link or "")).encode("utf-8")
    return hashlib.sha256(base).hexdigest()

def classify(title: str, source: str) -> str:
    n = normalize(title)
    s = (source or "").lower()

    # sports first
    if "sport" in s or any(k in n for k in SPORTS_KW):
        return "sports"

    # macro only if clearly macro
    if any(k in n for k in MACRO_KW):
        return "macro"

    return "breaking"

def why_it_matters(channel: str, title: str):
    n = normalize(title)

    if channel == "macro":
        if "inflation" in n or "cpi" in n or "ppi" in n:
            return [
                "Rate-cut timing may move",
                "GBP volatility can pick up",
                "Mortgage pricing can reprice quickly"
            ]
        return [
            "Rate expectations may shift",
            "Borrowing costs stay sensitive",
            "GBP & equities can reprice on headlines"
        ]

    if channel == "sports":
        return [
            "Squad selection may change",
            "Momentum & fixtures are affected",
            "Availability updates can shift outcomes"
        ]

    return [
        "Fast-moving situation",
        "Follow-up updates likely",
        "Knock-on impact possible"
    ]

def format_msg(channel: str, headline: str, source: str) -> str:
    tag1, tag2 = CHANNEL_HASHTAGS[channel]
    why = why_it_matters(channel, headline)
    header = "🇬🇧 BREAKING" if channel == "breaking" else ("📊 MACRO" if channel == "macro" else "⚽ SPORTS")

    return "\n".join([
        header,
        "",
        (headline or "").upper(),
        "",
        "Why it matters:",
        f"• {why[0]}",
        f"• {why[1]}",
        f"• {why[2]}",
        "",
        f"Source: {source}",
        "",
        f"{tag1} {tag2}",
    ])

async def send(channel: str, text: str):
    while True:
        try:
            await bot.send_message(
                chat_id=CHANNEL_CHAT_ID[channel],
                text=text,
                disable_web_page_preview=True
            )
            return
        except RetryAfter as e:
            wait_s = int(getattr(e, "retry_after", 10))
            print(f"Rate-limited by Telegram. Sleeping {wait_s}s…")
            await asyncio.sleep(wait_s + 1)


def fetch_items():
    items = []

    for url in BREAKING_FEEDS:
        d = feedparser.parse(url)
        src = d.feed.get("title", "RSS")
        for e in d.entries[:15]:
            items.append((src, e.get("title", "").strip(), e.get("link", "").strip()))

    for url in MACRO_FEEDS:
        d = feedparser.parse(url)
        src = d.feed.get("title", "RSS")
        for e in d.entries[:15]:
            items.append((src, e.get("title", "").strip(), e.get("link", "").strip()))

    for url in SPORTS_FEEDS:
        d = feedparser.parse(url)
        src = d.feed.get("title", "RSS")
        for e in d.entries[:15]:
            items.append((src, e.get("title", "").strip(), e.get("link", "").strip()))

    return items


async def main():
    db_init()
    print("UKNewsInsight_Bot started…")

    while True:
        recent_norms = db_recent_norms()

        for source, title, link in fetch_items():
            if not title:
                continue

            k = make_key(title, link)
            norm = normalize(title)

            # hard dedup: already posted
            if db_has_key(k):
                continue

            # soft dedup: similar to recent headlines
            if any(fuzz.token_set_ratio(norm, rn) >= SIM_THRESHOLD for rn in recent_norms):
                db_insert(k, norm)  # mark as seen to avoid future repeats
                continue

            ch = classify(title, source)

            await send(ch, format_msg(ch, title, source))
            db_insert(k, norm)

            # small throttle to avoid Telegram flood control
            await asyncio.sleep(2)

        await asyncio.sleep(POLL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
