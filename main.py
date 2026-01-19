import os
import re
import time
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

POLL_SECONDS  = int(os.getenv("POLL_SECONDS", "600"))   # 10 min default
SIM_THRESHOLD = int(os.getenv("SIM_THRESHOLD", "92"))   # stronger dedup (0-100)
RECENT_LIMIT  = int(os.getenv("RECENT_LIMIT", "500"))   # norms kept for soft-dedup

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

# MUST BE ONLY 2 hashtags at end
CHANNEL_HASHTAGS = {
    "breaking": ("#Breaking", "#UK"),
    "macro": ("#Macro", "#UK"),
    "sports": ("#Sports", "#UK"),
}

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

def clean_summary(summary: str) -> str:
    # RSS summaries sometimes contain HTML
    s = summary or ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def extract_text_payload(title: str, summary: str) -> str:
    return (title or "") + " " + (summary or "")

def detect_signals(n: str):
    """
    Returns list of signal tokens derived from the headline/summary text.
    These signals drive classification + impact reasoning.
    """
    signals = []

    # Macro / economy
    if any(x in n for x in ["inflation", "cpi", "ppi", "price index"]):
        signals.append("inflation")
    if any(x in n for x in ["gdp", "growth", "recession", "output"]):
        signals.append("growth")
    if any(x in n for x in ["interest rate", "rates", "rate cut", "rate hike", "bank of england", "boe"]):
        signals.append("rates")
    if any(x in n for x in ["unemployment", "jobs", "wages", "pay"]):
        signals.append("jobs")

    # Markets / trade
    if any(x in n for x in ["tariff", "sanction", "trade", "export", "import", "customs duty"]):
        signals.append("trade_policy")
    if any(x in n for x in ["bond", "yield", "gilt", "treasury", "spread"]):
        signals.append("rates_market")
    if any(x in n for x in ["gbp", "pound", "sterling"]):
        signals.append("fx_gbp")

    # Geopolitics / security
    if any(x in n for x in ["nato", "eu leaders", "european leaders", "white house", "downing street", "kremlin"]):
        signals.append("diplomacy")
    if any(x in n for x in ["war", "conflict", "attack", "missile", "drone", "troops"]):
        signals.append("security")
    if any(x in n for x in ["unacceptable", "condemn", "warning", "threat", "retaliat"]):
        signals.append("escalation_language")

    # Legal / regulator / investigations
    if any(x in n for x in ["accused", "investigation", "probe", "watchdog", "regulator", "court", "lawsuit", "charged"]):
        signals.append("investigation")

    # Consumer / business
    if any(x in n for x in ["restaurant", "delivery apps", "uber eats", "deliveroo", "just eat"]):
        signals.append("consumer_delivery")
    if any(x in n for x in ["big chains", "supermarket", "retailer", "brand"]):
        signals.append("consumer_retail")

    # Sports
    if any(x in n for x in ["premier league", "football", "match", "goal", "manager", "coach", "transfer", "injury"]):
        signals.append("sports")

    # Keep unique, stable order
    seen = set()
    out = []
    for s in signals:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

def classify(source: str, title: str, summary: str) -> str:
    n = normalize(extract_text_payload(title, summary))
    s = (source or "").lower()
    sig = detect_signals(n)

    # sports first (signal or source name)
    if "sport" in s or "sports" in s or ("sports" in sig):
        return "sports"

    # macro if macro signals exist
    if any(x in sig for x in ["inflation", "growth", "rates", "jobs", "rates_market", "fx_gbp"]):
        return "macro"

    return "breaking"

def build_insight_impact(channel: str, title: str, summary: str):
    """
    Returns (insight_line, impacts_list, signals_list)
    """
    n = normalize(extract_text_payload(title, summary))
    sig = detect_signals(n)

    # Default (safe)
    insight = "Developing story — details may evolve as updates come in."
    impacts = [
        "Watch for official statements / confirmations",
        "Follow-up headlines may add clarity",
        "Second-order impacts depend on next actions"
    ]

    if channel == "sports":
        insight = "Sports update — impact depends on availability, selection, and fixtures."
        impacts = [
            "Lineups / squad choices may shift",
            "Momentum & upcoming fixtures affected",
            "Follow-up updates likely (fitness, transfers, team news)"
        ]
        if "injury" in n:
            insight = "Injury-related update — availability may change short-term plans."
        if "transfer" in n:
            insight = "Transfer-related update — squad strength and tactics may shift."
        return insight, impacts, sig

    if channel == "macro":
        insight = "Macro headline — market reaction is driven by expectations vs actual outcomes."
        impacts = [
            "Rate expectations can move quickly",
            "GBP and yields may react to surprises",
            "Risk sentiment can shift across UK assets"
        ]

        if "inflation" in sig:
            insight = "Inflation signal — pricing pressure can change the rate path narrative."
            impacts = [
                "BoE rate-cut timing may shift",
                "GBP volatility can pick up",
                "Mortgage / borrowing costs may reprice"
            ]
        elif "growth" in sig:
            insight = "Growth signal — recession risk vs resilience can reprice risk appetite."
            impacts = [
                "Equities may reprice growth expectations",
                "Cyclicals/defensives rotation risk",
                "Consumer demand outlook may shift"
            ]
        elif "jobs" in sig:
            insight = "Jobs signal — wage pressure and slack affect inflation outlook."
            impacts = [
                "Rate path expectations may adjust",
                "GBP reaction possible",
                "Policy commentary risk increases"
            ]
        elif "fx_gbp" in sig:
            insight = "GBP signal — currency moves can amplify domestic inflation/financial conditions."
            impacts = [
                "FX-driven volatility risk",
                "Imported inflation narrative may shift",
                "Risk-on/risk-off positioning can change"
            ]

        return insight, impacts, sig

    # breaking
    insight = "Breaking headline — impact depends on policy response and escalation dynamics."
    impacts = [
        "Fast follow-ups likely as officials respond",
        "Policy/regulatory steps can change direction quickly",
        "Knock-on impacts possible across markets and public sentiment"
    ]

    if "trade_policy" in sig:
        insight = "Trade policy signal — tariffs/sanctions can change pricing and retaliation risk."
        impacts = [
            "Trade flows & prices may shift",
            "Retaliation / escalation risk rises",
            "Markets may reprice policy uncertainty"
        ]

    if "investigation" in sig:
        insight = "Investigation/regulator signal — credibility and compliance questions can escalate."
        impacts = [
            "Regulatory scrutiny may increase",
            "Consumer trust implications",
            "Business operations/pricing could be affected"
        ]

    if "diplomacy" in sig or "security" in sig or "escalation_language" in sig:
        insight = "Geopolitical signal — rhetoric and reactions can raise headline risk premium."
        impacts = [
            "Diplomatic escalation risk",
            "Policy response / statements likely",
            "Follow-up headlines can move sentiment fast"
        ]

    if "consumer_delivery" in sig or "consumer_retail" in sig:
        insight = "Consumer/business signal — brand positioning and transparency can face scrutiny."
        impacts = [
            "Consumer trust & pricing impact possible",
            "Regulatory/industry response may follow",
            "Competitors may change tactics quickly"
        ]

    return insight, impacts, sig

def format_msg(channel: str, headline: str, summary: str, source: str) -> str:
    tag1, tag2 = CHANNEL_HASHTAGS[channel]
    header = "🇬🇧 BREAKING" if channel == "breaking" else ("📊 MACRO" if channel == "macro" else "⚽ SPORTS")

    insight, impacts, sig = build_insight_impact(channel, headline, summary)

    # Keep summary short (optional line)
    summary_clean = clean_summary(summary)
    summary_line = ""
    if summary_clean:
        # limit length to avoid walls of text
        if len(summary_clean) > 180:
            summary_clean = summary_clean[:177].rstrip() + "…"
        summary_line = f"\nContext: {summary_clean}\n"

    signals_line = ""
    if sig:
        signals_line = f"\nSignals detected: {', '.join(sig[:6])}\n"

    return "\n".join([
        header,
        "",
        headline.strip(),
        summary_line.strip(),
        "Insight:",
        f"• {insight}",
        signals_line.strip(),
        "Impact (why it matters):",
        f"• {impacts[0]}",
        f"• {impacts[1]}",
        f"• {impacts[2]}",
        "",
        f"Source: {source}",
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

                # RSS usually has one of these; not always
                summary = (
                    e.get("summary")
                    or e.get("description")
                    or e.get("subtitle")
                    or ""
                )
                summary = clean_summary(summary)

                out.append((src, title, link, summary))
        return out

    items += pull(BREAKING_FEEDS)
    items += pull(MACRO_FEEDS)
    items += pull(SPORTS_FEEDS)

    return items


# ---------------- MAIN LOOP ----------------
async def main():
    db_init()
    print("UKNewsInsight_Bot started…")

    while True:
        recent_norms = db_recent_norms(RECENT_LIMIT)

        for source, title, link, summary in fetch_items():
            if not title:
                continue

            k = make_key(title, link)

            payload = extract_text_payload(title, summary)
            norm = normalize(payload)

            # hard dedup: already posted
            if db_has_key(k):
                continue

            # soft dedup: similar to recent headlines/summaries
            if any(fuzz.token_set_ratio(norm, rn) >= SIM_THRESHOLD for rn in recent_norms):
                db_insert(k, norm)  # mark as seen to avoid future repeats
                continue

            ch = classify(source, title, summary)

            msg = format_msg(ch, title, summary, source)
            await send(ch, msg)
            db_insert(k, norm)

            # small throttle to avoid flood control
            await asyncio.sleep(2)

        await asyncio.sleep(POLL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
