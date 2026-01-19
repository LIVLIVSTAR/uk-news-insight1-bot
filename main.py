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

POLL_SECONDS  = int(os.getenv("POLL_SECONDS", "600"))   # 10 min
SIM_THRESHOLD = int(os.getenv("SIM_THRESHOLD", "92"))
RECENT_LIMIT  = int(os.getenv("RECENT_LIMIT", "500"))

# keep ONLY 2 hashtags at the end of each post (your network standard)
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

def db_recent_norms(limit: int = 500):
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

def clean_html(text: str) -> str:
    s = text or ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def is_uk_related(n: str) -> bool:
    return any(x in n for x in [
        "uk", "britain", "british", "england", "scotland", "wales", "northern ireland",
        "london", "westminster", "downing street", "parliament", "bank of england", "boe"
    ])

def extract_summary(entry) -> str:
    return clean_html(
        entry.get("summary")
        or entry.get("description")
        or entry.get("subtitle")
        or ""
    )

# ---- KEYWORD SETS (strict macro; broad breaking) ----
SPORTS_KW = [
    "premier league", "football", "match", "goal", "injury", "transfer",
    "manager", "coach", "champions league", "fa cup", "rugby", "cricket", "tennis"
]

MACRO_KW = [
    "inflation", "cpi", "ppi", "gdp", "interest rate", "rates", "rate cut", "rate hike",
    "bank of england", "boe", "unemployment", "wages", "pay growth", "recession",
    "bond", "yield", "gilt", "sterling", "gbp", "budget", "tax", "tariff", "sanction"
]

# optional: tickers/hashtags like #COIN #CRCL (can be switched off)
ENABLE_TICKER_TAGS = os.getenv("ENABLE_TICKER_TAGS", "1") == "1"

def extract_ticker_tags(title: str):
    if not ENABLE_TICKER_TAGS:
        return []
    # crude but useful: ALLCAPS tokens 2-6 chars, avoid common words
    bad = {"UK", "US", "EU", "NATO", "BBC", "ONS", "PM", "MP"}
    tokens = re.findall(r"\b[A-Z]{2,6}\b", title or "")
    out = []
    for t in tokens:
        if t not in bad and t not in out:
            out.append(t)
    return out[:2]  # max 2 ticker tags


def classify(source: str, title: str, summary: str) -> str:
    """
    STRICT:
      - sports if sports keywords hit
      - macro only if macro keywords hit
      - else breaking
    """
    n = normalize(f"{title} {summary}")
    s = (source or "").lower()

    if "sport" in s or any(k in n for k in SPORTS_KW):
        return "sports"

    if any(k in n for k in MACRO_KW):
        return "macro"

    return "breaking"


def topic_tags(channel: str, title: str, summary: str):
    """
    Internal tags for header line (not the final 2 hashtags).
    Keep short; mimic MarketTwits vibe.
    """
    n = normalize(f"{title} {summary}")
    tags = []

    # geography / context
    if is_uk_related(n):
        tags.append("британия")
    else:
        tags.append("world")

    # topic buckets
    if any(x in n for x in ["tariff", "sanction", "trade war", "trade"]):
        tags.append("торговыевойны")
    if any(x in n for x in ["trump", "biden", "eu", "nato", "russia", "china", "iran", "ukraine", "zelensky"]):
        tags.append("геополитика")
    if any(x in n for x in ["stocks", "shares", "equities", "bonds", "yield", "gilt"]):
        tags.append("акции")
    if any(x in n for x in ["crypto", "bitcoin", "ethereum", "coinbase", "circle", "stablecoin", "blockchain"]):
        tags.append("крипто")
    if any(x in n for x in ["inflation", "cpi", "ppi", "gdp", "recession", "rates", "bank of england", "boe"]):
        tags.append("экономика")
    if any(x in n for x in ["attack", "war", "missile", "bomb", "shooting", "abduct", "kidnap", "terror"]):
        tags.append("security")

    if channel == "sports" and "спорт" not in tags:
        tags.append("спорт")

    # keep unique, max 4
    uniq = []
    for t in tags:
        if t not in uniq:
            uniq.append(t)
    return uniq[:4]


def header_line(channel: str, title: str, summary: str):
    """
    Example like screenshot:
    ⚠️ 🇬🇧 #экономика #британия
    """
    icon = {"breaking": "❗", "macro": "⚠️", "sports": "⚽"}.get(channel, "❗")
    n = normalize(f"{title} {summary}")
    flag = "🇬🇧" if is_uk_related(n) else "🌍"

    tags = topic_tags(channel, title, summary)
    # header tags are in Russian like your screenshot
    tag_str = " ".join(f"#{t}" for t in tags)
    return f"{icon} {flag} {tag_str}".strip()


def format_post(channel: str, title: str, source: str, summary: str):
    """
    MarketTwits style:
    Header line (icon + flag + a few topic tags)
    One-liner (title — source)
    Optional: 1 short context line if summary is clearly useful
    End: EXACTLY 2 network hashtags
    """
    tag1, tag2 = CHANNEL_HASHTAGS[channel]

    t = (title or "").strip()
    s = (source or "RSS").strip()

    # optional context: only if summary adds real info and isn't junk
    ctx = clean_html(summary)
    ctx_line = ""
    if ctx:
        # keep it very short
        if len(ctx) > 140:
            ctx = ctx[:137].rstrip() + "…"
        # avoid repeating title words too much (simple check)
        if fuzz.partial_ratio(normalize(ctx), normalize(t)) < 85:
            ctx_line = f"\n{ctx}"

    tickers = extract_ticker_tags(t)
    ticker_line = ""
    if tickers:
        ticker_line = "\n" + " ".join(f"#{x}" for x in tickers)

    return "\n".join([
        header_line(channel, t, summary),
        "",
        f"{t} — {s}",
        ctx_line.strip(),
        ticker_line.strip(),
        "",
        f"{tag1} {tag2}",
    ]).replace("\n\n\n", "\n\n").strip()


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
            for e in d.entries[:15]:
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
    print("UKNews_MT_Style bot started…")

    while True:
        recent_norms = db_recent_norms(RECENT_LIMIT)

        for source, title, link, summary in fetch_items():
            if not title:
                continue

            k = make_key(title, link)
            norm = normalize(f"{title} {summary}")

            # hard dedup
            if db_has_key(k):
                continue

            # soft dedup (similar to last N items)
            if any(fuzz.token_set_ratio(norm, rn) >= SIM_THRESHOLD for rn in recent_norms):
                db_insert(k, norm)  # mark as seen
                continue

            ch = classify(source, title, summary)

            msg = format_post(ch, title, source, summary)
            await send(ch, msg)

            db_insert(k, norm)

            # small throttle
            await asyncio.sleep(2)

        await asyncio.sleep(POLL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
