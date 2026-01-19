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

POLL_SECONDS     = int(os.getenv("POLL_SECONDS", "600"))   # 10 min default
SIM_THRESHOLD    = int(os.getenv("SIM_THRESHOLD", "92"))   # 0..100
RECENT_LIMIT     = int(os.getenv("RECENT_LIMIT", "600"))   # soft-dedup memory
EVENT_TTL_HOURS  = int(os.getenv("EVENT_TTL_HOURS", "24")) # event-level dedup window

# Keep ONLY 2 hashtags at end of each post (network standard)
CHANNEL_HASHTAGS = {
    "breaking": ("#Breaking", "#UK"),
    "macro":    ("#Macro", "#UK"),
    "sports":   ("#Sports", "#UK"),
}

CHANNEL_CHAT_ID = {
    "breaking": BREAKING_CHAT_ID,
    "macro":    MACRO_CHAT_ID,
    "sports":   SPORTS_CHAT_ID,
}

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


# ---------------- DB (persistent dedup) ----------------
DB_PATH = "state.db"

def db_init():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # hard dedup (title+link)
    cur.execute("""
      CREATE TABLE IF NOT EXISTS posted (
        k TEXT PRIMARY KEY,
        norm TEXT,
        ts TEXT
      )
    """)

    # event-level dedup (story family)
    cur.execute("""
      CREATE TABLE IF NOT EXISTS events (
        ek TEXT PRIMARY KEY,
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

def db_insert_posted(k: str, norm: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO posted (k, norm, ts) VALUES (?, ?, ?)",
        (k, norm, datetime.now(timezone.utc).isoformat())
    )
    con.commit()
    con.close()

def db_recent_norms(limit: int = 600):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT norm FROM posted ORDER BY ts DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    con.close()
    return [r[0] for r in rows if r and r[0]]

def db_event_seen_recent(ek: str) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT 1 FROM events
        WHERE ek = ?
          AND ts >= datetime('now', ?)
        LIMIT 1
    """, (ek, f"-{EVENT_TTL_HOURS} hours"))
    row = cur.fetchone()
    con.close()
    return row is not None

def db_event_touch(ek: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO events (ek, ts) VALUES (?, ?)",
        (ek, datetime.now(timezone.utc).isoformat())
    )
    con.commit()
    con.close()

def db_events_cleanup():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM events WHERE ts < datetime('now', ?)", (f"-{EVENT_TTL_HOURS} hours",))
    con.commit()
    con.close()


# ---------------- HELPERS ----------------
def normalize(text: str) -> str:
    t = (text or "").lower()
    t = re.sub(r"[\W_]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def make_key(title: str, link: str) -> str:
    base = (normalize(title) + "|" + (link or "")).encode("utf-8")
    return hashlib.sha256(base).hexdigest()

def clean_html(text: str) -> str:
    s = text or ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def extract_summary(entry) -> str:
    return clean_html(
        entry.get("summary")
        or entry.get("description")
        or entry.get("subtitle")
        or ""
    )

# Strict-ish keywords
SPORTS_KW = [
    "premier league", "football", "match", "goal", "injury", "transfer",
    "manager", "coach", "champions league", "fa cup", "rugby", "cricket", "tennis",
]

MACRO_KW = [
    "inflation", "cpi", "ppi", "gdp", "interest rate", "rates", "rate cut", "rate hike",
    "bank of england", "boe", "unemployment", "wages", "pay growth", "recession",
    "bond", "yield", "gilt", "sterling", "gbp", "budget", "tax",
]

def classify(source: str, title: str, summary: str) -> str:
    n = normalize(f"{title} {summary}")
    s = (source or "").lower()

    if "sport" in s or any(k in n for k in SPORTS_KW):
        return "sports"

    if any(k in n for k in MACRO_KW):
        return "macro"

    return "breaking"


# ---------------- EVENT-LEVEL DEDUP ----------------
STOPWORDS = {
    # common news filler words (EN)
    "watch", "live", "update", "latest", "video", "photos", "explained",
    "what", "we", "know", "about", "why", "how", "after", "as", "at", "in",
    "on", "to", "of", "and", "a", "an", "the", "for", "with", "from",
    "says", "say", "told", "report", "reports", "scene", "new", "more",
    "than", "least", "near", "city", "worst", "disaster", "decade",
}

def make_event_key(title: str) -> str:
    """
    Group related headlines about the same story.
    Designed to suppress BBC-style multi-posts (Watch / What we know / survivors say...)
    """
    t = normalize(title)

    # remove common prefix patterns
    t = re.sub(r"^(watch|live|update)\s*:\s*", "", t).strip()
    t = re.sub(r"^what we know about\s+", "", t).strip()

    tokens = [x for x in t.split() if len(x) >= 4 and x not in STOPWORDS]

    # keep strongest tokens only; sort for order-invariance
    tokens = sorted(set(tokens[:12]))

    base = " ".join(tokens)[:140]
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


# ---------------- FORMAT (NO EMOJIS / NO EXTRA TAGS) ----------------
def format_post(channel: str, title: str, source: str, summary: str) -> str:
    tag1, tag2 = CHANNEL_HASHTAGS[channel]

    t = (title or "").strip()
    s = (source or "RSS").strip()

    ctx = clean_html(summary)
    ctx_line = ""
    if ctx:
        # keep context short, and only if it adds something different from title
        if len(ctx) > 180:
            ctx = ctx[:177].rstrip() + "…"
        if fuzz.partial_ratio(normalize(ctx), normalize(t)) < 85:
            ctx_line = ctx

    parts = [
        t,
        f"— {s}",
    ]
    first_line = " ".join(parts).strip()

    if ctx_line:
        body = first_line + "\n" + ctx_line
    else:
        body = first_line

    return "\n".join([
        body.strip(),
        "",
        f"{tag1} {tag2}",
    ]).strip()


# ---------------- TELEGRAM SEND ----------------
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
        except Exception as e:
            print(f"Telegram send error: {e}")
            await asyncio.sleep(5)


# ---------------- FETCH RSS ITEMS ----------------
def fetch_items():
    """
    Returns list of tuples: (source_title, title, link, summary)
    """
    items = []

    def pull(urls):
        out = []
        for url in urls:
            d = feedparser.parse(url)
            src = d.feed.get("title", "RSS")
            for e in d.entries[:20]:
                title = (e.get("title") or "").strip()
                link  = (e.get("link") or "").strip()
                summary = extract_summary(e)
                out.append((src, title, link, summary))
        return out

    items += pull(BREAKING_FEEDS)
    items += pull(MACRO_FEEDS)
    items += pull(SPORTS_FEEDS)

    return items


# ---------------- MAIN LOOP ----------------
async def main():
    db_init()
    print("UKNews bot started… (clean style, event dedup enabled)")

    while True:
        db_events_cleanup()
        recent_norms = db_recent_norms(RECENT_LIMIT)

        for source, title, link, summary in fetch_items():
            if not title:
                continue

            k = make_key(title, link)
            norm = normalize(f"{title} {summary}")

            # hard dedup
            if db_has_key(k):
                continue

            # soft dedup (similar to recent)
            if any(fuzz.token_set_ratio(norm, rn) >= SIM_THRESHOLD for rn in recent_norms):
                db_insert_posted(k, norm)
                continue

            # event-level dedup (story family)
            ek = make_event_key(title)
            if db_event_seen_recent(ek):
                db_insert_posted(k, norm)
                continue

            ch = classify(source, title, summary)
            msg = format_post(ch, title, source, summary)

            await send(ch, msg)

            db_insert_posted(k, norm)
            db_event_touch(ek)

            # small throttle to avoid Telegram flood control
            await asyncio.sleep(2)

        await asyncio.sleep(POLL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
