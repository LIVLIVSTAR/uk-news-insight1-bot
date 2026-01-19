import os
import time
import hashlib
import re
from datetime import datetime, timezone, timedelta

import feedparser
from rapidfuzz import fuzz
from telegram import Bot

# ---------------- CONFIG ----------------
BREAKING_CHAT_ID = int(os.getenv("BREAKING_CHAT_ID", "-1003118967605"))
MACRO_CHAT_ID    = int(os.getenv("MACRO_CHAT_ID", "-1003153326236"))
SPORTS_CHAT_ID   = int(os.getenv("SPORTS_CHAT_ID", "-1001803566974"))

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN env var")

bot = Bot(token=BOT_TOKEN)

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
LOCKOUT_HOURS = int(os.getenv("LOCKOUT_HOURS", "36"))
SIM_THRESHOLD = int(os.getenv("SIM_THRESHOLD", "88"))
RECENT_WINDOW_HOURS = int(os.getenv("RECENT_WINDOW_HOURS", "48"))

BREAKING_FEEDS = [
    "http://newsrss.bbc.co.uk/rss/newsonline_uk_edition/front_page/rss.xml",
    "http://newsrss.bbc.co.uk/rss/newsonline_uk_edition/uk/rss.xml",
    "http://newsrss.bbc.co.uk/rss/newsonline_uk_edition/world/rss.xml",
    "http://newsrss.bbc.co.uk/rss/newsonline_uk_edition/uk_politics/rss.xml",
    "https://feeds.skynews.com/feeds/rss/home.xml",
    "https://feeds.skynews.com/feeds/rss/uk.xml",
    "https://feeds.skynews.com/feeds/rss/world.xml",
    "https://feeds.skynews.com/feeds/rss/politics.xml",
]

MACRO_FEEDS = [
    "http://newsrss.bbc.co.uk/rss/newsonline_uk_edition/business/rss.xml",
    "https://www.ons.gov.uk/ons/media-centre/rss.xml",
]

SPORTS_FEEDS = [
    "https://feeds.bbci.co.uk/sport/rss.xml",
]

CHANNEL_HASHTAGS = {
    "breaking": ("#Breaking", "#UK"),
    "macro": ("#Macro", "#UK"),
    "sports": ("#Sports", "#UK"),
}

CHANNEL_CHAT_ID = {
    "breaking": BREAKING_CHAT_ID,
    "macro": MACRO_CHAT_ID,
    "sports": SPORTS_CHAT_ID,
}

SPORTS_KW = [
    "premier league","goal","injury","transfer","match","manager","coach",
    "champions league","fa cup","efl","football","arsenal","chelsea","liverpool",
    "manchester united","manchester city","tottenham","newcastle","west ham"
]
MACRO_KW = [
    "inflation","cpi","ppi","gdp","rates","interest rate","bank of england","boe",
    "ons","unemployment","jobs","wages","bond","yield","sterling","gbp","economy"
]

posted = []

def now_utc():
    return datetime.now(timezone.utc)

def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[\W_]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = text.replace("boe", "bank of england")
    text = text.replace("man utd", "manchester united")
    return text

def story_key(norm: str) -> str:
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()

def is_duplicate(norm: str) -> bool:
    recent = [p for p in posted if p["at"] >= (now_utc() - timedelta(hours=RECENT_WINDOW_HOURS))]
    for p in recent:
        if p["key"] == story_key(norm):
            return True
        if fuzz.token_set_ratio(norm, p["norm"]) >= SIM_THRESHOLD:
            return True
    return False

def classify(title: str, source: str) -> str:
    norm = normalize(title)
    if "sport" in source.lower():
        return "sports"
    if any(k in norm for k in SPORTS_KW):
        return "sports"
    if any(k in norm for k in MACRO_KW):
        return "macro"
    return "breaking"

def why_it_matters(channel: str) -> list[str]:
    if channel == "macro":
        return [
            "Rate expectations may shift",
            "Mortgage and borrowing costs stay sensitive",
            "GBP and equities can reprice on the headline",
        ]
    if channel == "sports":
        return [
            "Squad selection and momentum are affected",
            "Fixture congestion risk increases",
            "Transfer pressure may build",
        ]
    return [
        "Public disruption is likely",
        "Authorities may issue follow-up updates",
        "Knock-on effects can spread across services",
    ]

def format_msg(channel: str, headline: str, source: str) -> str:
    tag1, tag2 = CHANNEL_HASHTAGS[channel]
    why = why_it_matters(channel)
    header = "🇬🇧 BREAKING" if channel == "breaking" else ("📊 MACRO" if channel == "macro" else "⚽ SPORTS")
    return "\n".join([
        header,
        "",
        headline.upper(),
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

def send(channel: str, text: str):
    bot.send_message(chat_id=CHANNEL_CHAT_ID[channel], text=text, disable_web_page_preview=True)

def fetch_feeds():
    items = []
    for url in BREAKING_FEEDS:
        d = feedparser.parse(url)
        for e in d.entries[:10]:
            items.append(("breaking", d.feed.get("title", "RSS"), e.get("title", "").strip()))
    for url in MACRO_FEEDS:
        d = feedparser.parse(url)
        for e in d.entries[:10]:
            items.append(("macro", d.feed.get("title", "RSS"), e.get("title", "").strip()))
    for url in SPORTS_FEEDS:
        d = feedparser.parse(url)
        for e in d.entries[:10]:
            items.append(("sports", d.feed.get("title", "RSS"), e.get("title", "").strip()))
    return items

print("UKNewsInsight_Bot started…")

while True:
    fetched = fetch_feeds()
    for default_channel, source, title in fetched:
        if not title:
            continue
        ch = classify(title, source)
        norm = normalize(title)
        if is_duplicate(norm):
            continue
        send(ch, format_msg(ch, title, source))
        posted.append({"key": story_key(norm), "norm": norm, "at": now_utc()})
    time.sleep(POLL_SECONDS)

