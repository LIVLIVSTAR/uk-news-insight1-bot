import asyncio
import hashlib
import logging
import os
import signal
from dataclasses import dataclass
from datetime import datetime, timedelta
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
logger = logging.getLogger("uk_news_bot")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
BREAKING_CHAT_ID = int(os.getenv("BREAKING_CHAT_ID", "0"))
MACRO_CHAT_ID = int(os.getenv("MACRO_CHAT_ID", "0"))
SPORTS_CHAT_ID = int(os.getenv("SPORTS_CHAT_ID", "0"))

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "600"))
SIM_THRESHOLD = float(os.getenv("SIM_THRESHOLD", "0.92"))
DB_PATH = os.getenv("DB_PATH", "news_bot.db")

RSS_FEEDS = [
    "https://feeds.bbci.co.uk/news/uk/rss.xml",
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://feeds.bbci.co.uk/sport/rss.xml",
    "https://feeds.skynews.com/feeds/rss/uk.xml",
    "https://feeds.skynews.com/feeds/rss/world.xml",
    "https://www.ons.gov.uk/rss",
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
        # Already stored or other non-fatal DB issue
        pass


# =========================
# CLASSIFICATION
# =========================

MACRO_KEYWORDS = [
    "inflation", "cpi", "ppi", "gdp", "recession",
    "interest rate", "interest rates", "bank of england", "boe",
    "bond yields", "gilts", "unemployment", "wages", "labour market",
    "labor market", "sterling", "gbp", "pound",
]

SPORTS_KEYWORDS = [
    "football", "premier league", "championship", "fa cup", "carabao cup",
    "goal", "goals", "match", "fixture", "injury", "injuries",
    "transfer", "transfers", "manager", "coach", "team", "club",
]

POLITICS_KEYWORDS = [
    "prime minister", "pm", "downing street", "parliament", "mp", "mps",
    "labour", "labour party", "conservative", "conservatives", "tory",
    "tories", "election", "by-election", "referendum", "government",
    "minister", "cabinet", "home office", "foreign office",
]

CELEBRITY_KEYWORDS = [
    "celebrity", "royal", "royals", "prince", "princess", "duke",
    "duchess", "king", "queen", "actor", "actress", "singer",
    "musician", "tv star", "reality star",
]


class Category:
    BREAKING = "BREAKING"
    MACRO = "MACRO"
    SPORTS = "SPORTS"


def classify_news(item: NewsItem) -> Category:
    text = f"{item.title} {item.source}".lower()

    def has(words: List[str]) -> bool:
        return any(w in text for w in words)

    # Sports first
    if has(SPORTS_KEYWORDS):
        return Category.SPORTS

    # Macro only if macro and not politics/celebrity
    if has(MACRO_KEYWORDS) and not (has(POLITICS_KEYWORDS) or has(CELEBRITY_KEYWORDS)):
        return Category.MACRO

    # Default: breaking
    return Category.BREAKING


def category_to_header_and_chat_id(category: Category) -> Tuple[str, int]:
    if category == Category.BREAKING:
        return "🇬🇧", BREAKING_CHAT_ID
    if category == Category.MACRO:
        return "📊", MACRO_CHAT_ID
    if category == Category.SPORTS:
        return "⚽", SPORTS_CHAT_ID
    return "🇬🇧", BREAKING_CHAT_ID


# =========================
# MESSAGE FORMAT (MARKETTWITS STYLE)
# =========================

def build_message(item: NewsItem, category: Category) -> str:
    emoji, _ = category_to_header_and_chat_id(category)
    title = item.title.strip()
    source = (item.source or "").strip().lower() or "unknown"
    hashtags = f"#{category.lower()} #uk"

    return (
        f"{emoji} {hashtags}\n"
        f"{title}\n\n"
        f"{source}"
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

    for item in channel.findall("item"):
        title_el = item.find("title")
        link_el = item.find("link")
        pub_el = item.find("pubDate")

        if title_el is None or link_el is None:
            continue

        title = title_el.text or ""
        link = link_el.text or ""

        published: Optional[datetime] = None
        if pub_el is not None and pub_el.text:
            try:
                from email.utils import parsedate_to_datetime
                published = parsedate_to_datetime(pub_el.text)
            except Exception:
                published = None

        source_el = channel.find("title")
        source_name = source_el.text if source_el is not None and source_el.text else url

        items.append(
            NewsItem(
                title=title.strip(),
                link=link.strip(),
                source=source_name.strip(),
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

    # Dedup within batch
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

async def send_message_with_retry(bot: Bot, chat_id: int, text: str) -> None:
    # basic spacing to avoid flood
    await asyncio.sleep(2)
    while True:
        try:
            await bot.send_message(chat_id=chat_id, text=text)
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

async def process_news_cycle(bot: Bot, conn: aiosqlite.Connection) -> None:
    logger.info("Fetching RSS feeds...")
    items = await fetch_all_feeds()
    logger.info("Fetched %d items from feeds", len(items))

    items.sort(key=lambda x: x.published or datetime.utcnow())

    for item in items:
        if not item.title or not item.link:
            continue

        if await is_duplicate(conn, item):
            continue

        category = classify_news(item)
        _, chat_id = category_to_header_and_chat_id(category)
        if chat_id == 0:
            logger.warning("Chat ID for category %s not configured, skipping", category)
            continue

        msg = build_message(item, category)
        logger.info("Posting [%s] %s", category, item.title)
        await send_message_with_retry(bot, chat_id, msg)
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

    logger.info("UK News bot started. Poll interval: %s seconds", POLL_SECONDS)

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
