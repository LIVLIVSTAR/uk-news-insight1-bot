import asyncio
import hashlib
import logging
import os
import signal
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

import aiohttp
import aiosqlite
from difflib import SequenceMatcher
from telegram import Bot
from telegram.error import RetryAfter, TimedOut, NetworkError

# =========================
# CONFIG
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("uk_single_channel_news_bot")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

# You can set either:
# 1) CHANNEL_CHAT_ID (recommended): numeric id like -100xxxxxxxxxx
# OR
# 2) CHANNEL_USERNAME: like @Uk_breaking_newss
CHANNEL_CHAT_ID = os.getenv("CHANNEL_CHAT_ID", "").strip()
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "").strip()

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "600"))
SIM_THRESHOLD = float(os.getenv("SIM_THRESHOLD", "0.92"))
DB_PATH = os.getenv("DB_PATH", "news_bot.db")

# Impact threshold (higher = fewer posts, cleaner channel)
IMPACT_THRESHOLD = float(os.getenv("IMPACT_THRESHOLD", "3.0"))

# Boot protection: do not post old news after restart
BOOT_LOOKBACK_MINUTES = int(os.getenv("BOOT_LOOKBACK_MINUTES", "10"))  # minutes
BOT_STARTED_AT = datetime.now(timezone.utc)

# =========================
# RSS FEEDS
# =========================

RSS_FEEDS = [
    # ===== REUTERS (mirrors, UK locked) =====
    "https://news.google.com/rss/search?q=site:reuters.com+UK&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=site:reuters.com+Bank+of+England&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=site:reuters.com+UK+inflation&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=site:reuters.com+UK+employment&hl=en-GB&gl=GB&ceid=GB:en",

    # ===== BLOOMBERG UK =====
    "https://news.google.com/rss/search?q=site:bloomberg.com+UK&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=site:bloomberg.com+Bank+of+England&hl=en-GB&gl=GB&ceid=GB:en",

    # ===== FINANCIAL TIMES =====
    "https://news.google.com/rss/search?q=site:ft.com+UK&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=site:ft.com+UK+economy&hl=en-GB&gl=GB&ceid=GB:en",

    # ===== GOV.UK =====
    "https://news.google.com/rss/search?q=site:gov.uk+policy&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=site:gov.uk+consultation&hl=en-GB&gl=GB&ceid=GB:en",

    # ===== BANK OF ENGLAND =====
    "https://news.google.com/rss/search?q=site:bankofengland.co.uk&hl=en-GB&gl=GB&ceid=GB:en",

    # ===== GUARDIAN =====
    "https://www.theguardian.com/politics/rss",
    "https://www.theguardian.com/uk/business/rss",

    # ===== BBC / SKY =====
    "https://feeds.bbci.co.uk/news/uk/rss.xml",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://feeds.skynews.com/feeds/rss/uk.xml",
]





FUZZY_LOOKBACK_MINUTES = 180


@dataclass
class NewsItem:
    title: str
    link: str
    source: str
    published: Optional[datetime]


# =========================
# DATABASE & DEDUP
# =========================

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS news_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hash TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    link TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);
"""

INSERT_ITEM_SQL = """
INSERT INTO news_items (hash, title, link, created_at)
VALUES (?, ?, ?, ?);
"""

SELECT_RECENT_TITLES_SQL = """
SELECT title FROM news_items
WHERE created_at >= ?;
"""


async def init_db(db_path: str = DB_PATH) -> aiosqlite.Connection:
    conn = await aiosqlite.connect(db_path)
    await conn.execute(CREATE_TABLE_SQL)
    await conn.commit()
    return conn


def normalize_text(text: str) -> str:
    return " ".join(text.lower().strip().split())


def compute_hash(title: str, link: str) -> str:
    normalized = normalize_text(title) + "|" + link.strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


async def is_duplicate(conn: aiosqlite.Connection, item: NewsItem) -> bool:
    h = compute_hash(item.title, item.link)

    # Exact hash
    cursor = await conn.execute("SELECT 1 FROM news_items WHERE hash = ?;", (h,))
    row = await cursor.fetchone()
    await cursor.close()
    if row:
        return True

    # Fuzzy similarity on recent titles
    cutoff = datetime.utcnow() - timedelta(minutes=FUZZY_LOOKBACK_MINUTES)
    cursor = await conn.execute(SELECT_RECENT_TITLES_SQL, (cutoff.isoformat(),))
    rows = await cursor.fetchall()
    await cursor.close()

    new_norm = normalize_text(item.title)
    for (existing,) in rows:
        ratio = SequenceMatcher(None, new_norm, normalize_text(existing)).ratio()
        if ratio >= SIM_THRESHOLD:
            return True

    return False


async def store_item(conn: aiosqlite.Connection, item: NewsItem) -> None:
    h = compute_hash(item.title, item.link)
    try:
        await conn.execute(
            INSERT_ITEM_SQL,
            (h, item.title, item.link, datetime.utcnow().isoformat()),
        )
        await conn.commit()
    except Exception:
        # already stored or non-fatal db issue
        pass


# =========================
# FILTERING (WHITELIST / BLACKLIST / IMPACT)
# =========================

WHITELIST = [
    # ===== CRYPTO / METALS / PRICES =====
    "btc", "bitcoin", "crypto", "cryptocurrency",
    "gold", "silver",
    "price", "prices", "high prices", "cost of living",
    "inflation", "cpi", "ppi",

    # ===== MACRO / LABOUR =====
    "employment", "jobs", "job market", "unemployment",
    "wages", "salary", "salaries", "pay", "pay growth",
    "labour market",

    # ===== BENEFITS / SOCIAL =====
    "universal credit", "benefits", "pension", "pensions",
    "state pension", "triple lock",
    "disability benefits", "housing benefit",

    # ===== MONEY / MARKETS =====
    "bond", "bonds", "gilts", "yield", "yields", "gilt yields",
    "ftse", "ftse 100", "ftse 250",
    "stocks", "equities", "shares",
    "sterling", "gbp", "pound", "exchange rate",
    "interest rate", "interest rates",
    "bank of england", "boe",

    # ===== TAX / BUDGET =====
    "budget", "spring budget", "autumn statement",
    "tax", "tax cut", "tax rise",
    "hm treasury", "hmrc",

    # ===== ENERGY =====
    "energy", "energy bills", "energy cap",
    "gas prices", "electricity prices",
    "ofgem", "utilities",

    # ===== HOUSING =====
    "mortgage", "mortgages", "housing",
    "house prices", "rent", "rents",
    "landlord", "renters",
    "leasehold", "freehold",
    "rightmove", "zoopla",

    # ===== GOVERNMENT / REGULATION =====
    "consultation", "white paper", "green paper",
    "regulation", "regulator",
    "policy", "reform", "bill", "law",

    # ===== IMMIGRATION / SECURITY =====
    "immigration", "asylum",
    "small boats", "boat crossings", "boats",
    "border", "home office",

    # ===== GEOPOLITICS (UK-RELEVANT) =====
    "greenland", "war", "conflict",
    "sanctions", "nato", "defence",
    "national security",
    "mi5", "gchq",

    # ===== STRIKES / INFRASTRUCTURE =====
    "strike", "strikes",
    "rail strike", "train strike",
    "junior doctors", "nurses",
    "nhs", "waiting times",
    "rail", "trains", "tfl", "hs2",

    # ===== POLITICS =====
    "election", "polls",
    "labour", "conservative", "reform uk",
    "prime minister", "downing street",
    "parliament", "westminster",

    # ===== MAJOR UK COMPANIES =====
    "bp", "shell", "hsbc", "barclays", "lloyds",
    "tesco", "sainsbury", "vodafone",
    "bt", "rolls-royce",
]



BLACKLIST = [
    # human interest / tragedy / noise
    "tumour", "tumor", "teenage", "brain", "inoperable", "cancer",
    "missing", "abduction", "kidnapped", "kidnap",
    "injured", "injuries", "dead", "died", "killed", "murder", "rape",
    "police said", "police say", "arrested", "shooting", "stabbing",
    "church",
    "denied reports",
    # often non-UK geopolitical crime/war noise
    "mass abduction", "gunman", "militants",
]

UK_HINTS = [
    "uk", "britain", "british", "england", "scotland", "wales", "northern ireland",
    "london", "westminster", "downing street", "hmrc", "ons", "bank of england",
    "sterling", "gbp", "pound",
]

HIGH_SIGNAL_SOURCES = [
    "office for national statistics",
    "bank of england",
    "gov.uk",
    "institute for fiscal studies",
    "resolution foundation",
    "bbc news - uk",
    "bbc news - business",
    "sky news - uk",
    "the guardian",
]


def text_contains_any(text: str, needles: List[str]) -> bool:
    t = text.lower()
    return any(n in t for n in needles)


def has_numbers(text: str) -> bool:
    return bool(re.search(r"(\d+(\.\d+)?)|(%|£|\$)", text))


def impact_score(item: NewsItem) -> float:
    title = item.title.lower()
    source = (item.source or "").lower()
    full = f"{title} {source}"

    score = 0.0

    if text_contains_any(full, BLACKLIST):
        score -= 3.0

    if text_contains_any(full, WHITELIST):
        score += 2.0

    if text_contains_any(full, UK_HINTS):
        score += 1.0

    if has_numbers(item.title):
        score += 1.0

    if any(s in source for s in HIGH_SIGNAL_SOURCES):
        score += 1.0

    macro_triggers = [
        "cpi", "ppi", "inflation", "unemployment", "wages",
        "bank of england", "boe", "gdp", "pmi", "budget", "mortgage"
    ]
    if text_contains_any(full, macro_triggers):
        score += 1.0

    return score


def should_publish(item: NewsItem) -> Tuple[bool, float, str]:
    full = f"{item.title} {item.source}".lower()

    bl = text_contains_any(full, BLACKLIST)
    wl = text_contains_any(full, WHITELIST)

    score = impact_score(item)

    if bl and not wl and score < IMPACT_THRESHOLD:
        return False, score, "blacklist"

    if not wl and score < IMPACT_THRESHOLD:
        return False, score, "low_impact"

    return True, score, "ok"


# =========================
# MESSAGE FORMAT (CLEAN, 2 HASHTAGS)
# =========================

def build_message(item: NewsItem, score: float) -> str:
    hashtags = "#UK #News"
    title = item.title.strip()
    source = (item.source or "").strip()

    return (
        f"🇬🇧 {hashtags}\n"
        f"{title}\n\n"
        f"{source}\n"
        f"{item.link}\n"
        f"Impact: {score:.1f}"
    )


# =========================
# RSS FETCHING
# =========================

async def fetch_rss_feed(session: aiohttp.ClientSession, url: str) -> List[NewsItem]:
    items: List[NewsItem] = []
    try:
        async with session.get(url, timeout=20) as resp:
            resp.raise_for_status()
            text = await resp.text()
    except Exception as e:
        logger.warning("Failed to fetch RSS feed %s: %s", url, e)
        return items

    from xml.etree import ElementTree as ET

    try:
        root = ET.fromstring(text)
    except Exception as e:
        logger.warning("Failed to parse RSS feed %s: %s", url, e)
        return items

    channel = root.find("channel")
    if channel is None:
        channel = root

    channel_title_el = channel.find("title")
    channel_title = channel_title_el.text.strip() if (channel_title_el is not None and channel_title_el.text) else url

    for it in channel.findall("item"):
        title_el = it.find("title")
        link_el = it.find("link")
        pub_el = it.find("pubDate")

        if title_el is None or link_el is None:
            continue

        title = (title_el.text or "").strip()
        link = (link_el.text or "").strip()

        if not title or not link:
            continue

        published: Optional[datetime] = None
        if pub_el is not None and pub_el.text:
            try:
                from email.utils import parsedate_to_datetime
                published = parsedate_to_datetime(pub_el.text)
            except Exception:
                published = None

        items.append(
            NewsItem(
                title=title,
                link=link,
                source=channel_title,
                published=published,
            )
        )

    return items


async def fetch_all_feeds() -> List[NewsItem]:
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_rss_feed(session, url) for url in RSS_FEEDS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    all_items: List[NewsItem] = []
    for res in results:
        if isinstance(res, list):
            all_items.extend(res)

    seen = set()
    unique: List[NewsItem] = []
    for item in all_items:
        key = (item.title, item.link)
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)

    return unique


# =========================
# TELEGRAM SENDER
# =========================

def resolve_channel_target() -> Tuple[Optional[int], Optional[str]]:
    cid: Optional[int] = None
    if CHANNEL_CHAT_ID:
        try:
            cid = int(CHANNEL_CHAT_ID)
        except ValueError:
            cid = None

    uname = CHANNEL_USERNAME.strip()
    if uname and not uname.startswith("@"):
        uname = f"@{uname}"

    return cid, (uname or None)


async def send_message_with_retry(bot: Bot, chat_id: Optional[int], username: Optional[str], text: str) -> None:
    await asyncio.sleep(1.5)
    while True:
        try:
            target = chat_id if chat_id is not None else username
            if not target:
                logger.error("No channel target configured. Set CHANNEL_CHAT_ID or CHANNEL_USERNAME.")
                return
            await bot.send_message(chat_id=target, text=text, disable_web_page_preview=False)
            break
        except RetryAfter as e:
            delay = getattr(e, "retry_after", 5)
            logger.warning("RetryAfter from Telegram, sleeping for %s seconds", delay)
            await asyncio.sleep(delay)
        except (TimedOut, NetworkError) as e:
            logger.warning("Network/timeout error sending message: %s; retrying in 5s", e)
            await asyncio.sleep(5)
        except Exception as e:
            logger.exception("Unexpected error sending message: %s", e)
            break


# =========================
# MAIN LOOP
# =========================

def _normalize_published_dt(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def process_news_cycle(bot: Bot, conn: aiosqlite.Connection) -> None:
    logger.info("Fetching RSS feeds...")
    items = await fetch_all_feeds()
    logger.info("Fetched %d items from feeds", len(items))

    # Normalize published times and sort
    for i in items:
        i.published = _normalize_published_dt(i.published)

    items.sort(key=lambda x: x.published or datetime.now(timezone.utc))

    chat_id, username = resolve_channel_target()

    for item in items:
        if not item.title or not item.link:
            continue

        # Dedup (DB + fuzzy)
        if await is_duplicate(conn, item):
            continue

        # --- BOOT LOCKOUT: skip old items after restart (anti-flood) ---
        if item.published is not None:
            boot_cutoff = BOT_STARTED_AT - timedelta(minutes=BOOT_LOOKBACK_MINUTES)
            if item.published < boot_cutoff:
                logger.info("SKIP (boot_lockout %sm): %s", BOOT_LOOKBACK_MINUTES, item.title)
                # still store so we never post it later
                await store_item(conn, item)
                continue

        ok, score, reason = should_publish(item)
        if not ok:
            logger.info("SKIP (reason=%s score=%.1f): %s", reason, score, item.title)
            await store_item(conn, item)
            continue

        msg = build_message(item, score)
        logger.info("POST (score=%.1f): %s", score, item.title)
        await send_message_with_retry(bot, chat_id, username, msg)
        await store_item(conn, item)


async def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN is not set. Exiting.")
        return

    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    conn = await init_db(DB_PATH)

    stop_event = asyncio.Event()

    def handle_signal(sig, frame):
        logger.info("Received signal %s, shutting down...", sig)
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info(
        "UK Single-Channel News bot started. Poll interval: %s sec | Impact threshold: %.1f | Boot lockout: %s min",
        POLL_SECONDS, IMPACT_THRESHOLD, BOOT_LOOKBACK_MINUTES
    )

    try:
        while not stop_event.is_set():
            try:
                await process_news_cycle(bot, conn)
            except Exception as e:
                logger.exception("Error in processing cycle: %s", e)

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=POLL_SECONDS)
            except asyncio.TimeoutError:
                pass
    finally:
        await conn.close()
        logger.info("Bot stopped.")


if __name__ == "__main__":
    asyncio.run(main())
