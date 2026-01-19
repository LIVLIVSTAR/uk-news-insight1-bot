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
# DATABASE
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

    cursor = await conn.execute("SELECT 1 FROM news_items WHERE hash = ?;", (h,))
    row = await cursor.fetchone()
    await cursor.close()
    if row:
        return True

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
        pass


# =========================
# CLASSIFICATION
# =========================

MACRO_KEYWORDS = [
    "inflation", "cpi", "ppi", "gdp", "recession",
    "interest rate", "interest rates", "bank of england", "boe",
    "bond yields", "gilts", "unemployment", "wages", "labour market",
    "sterling", "gbp", "pound",
]

SPORTS_KEYWORDS = [
    "football", "premier league", "goal", "match", "fixture",
    "injury", "transfer", "manager", "coach", "team", "club",
]

POLITICS_KEYWORDS = [
    "prime minister", "pm", "parliament", "mp", "mps",
    "labour", "conservative", "tory", "election", "government",
]

CELEBRITY_KEYWORDS = [
    "royal", "prince", "princess", "king", "queen",
    "actor", "singer", "celebrity",
]


class Category:
    BREAKING = "BREAKING"
    MACRO = "MACRO"
    SPORTS = "SPORTS"


def classify_news(item: NewsItem) -> Category:
    text = f"{item.title} {item.source}".lower()

    def has(words): return any(w in text for w in words)

    if has(SPORTS_KEYWORDS):
        return Category.SPORTS

    if has(MACRO_KEYWORDS) and not (has(POLITICS_KEYWORDS) or has(CELEBRITY_KEYWORDS)):
        return Category.MACRO

    return Category.BREAKING


def category_to_header_and_chat_id(category: Category) -> Tuple[str, int]:
    if category == Category.BREAKING:
        return "🇬🇧 BREAKING", BREAKING_CHAT_ID
    if category == Category.MACRO:
        return "📊 MACRO", MACRO_CHAT_ID
    if category == Category.SPORTS:
        return "⚽ SPORTS", SPORTS_CHAT_ID
    return "🇬🇧 BREAKING", BREAKING_CHAT_ID


# =========================
# MESSAGE FORMAT (NO WHY IT MATTERS)
# =========================

def build_message(item: NewsItem, category: Category) -> str:
    header, _ = category_to_header_and_chat_id(category)
    title_upper = item.title.upper().strip()
    source_name = item.source.strip() or "Unknown"
    hashtag = f"#{category.capitalize()}"

    return (
        f"{header}\n\n"
        f"{title_upper}\n\n"
        f"Source: {source_name}\n\n"
        f"{hashtag} #UK"
    )


# =========================
# RSS FETCHING
# =========================

async def fetch_rss_feed(session: aiohttp.ClientSession, url: str) -> List[NewsItem]:
    items = []
    try:
        async with session.get(url, timeout=20) as resp:
            text = await resp.text()
    except Exception:
        return items

    from xml.etree import ElementTree as ET
    try:
        root = ET.fromstring(text)
    except Exception:
        return items

    channel = root.find("channel") or root

    for item in channel.findall("item"):
        title_el = item.find("title")
        link_el = item.find("link")
        pub_el = item.find("pubDate")

        if not title_el or not link_el:
            continue

        title = title_el.text or ""
        link = link_el.text or ""

        published = None
        if pub_el is not None:
            try:
                from email.utils import parsedate_to_datetime
                published = parsedate_to_datetime(pub_el.text)
            except Exception:
                pass

        source_el = channel.find("title")
        source_name = source_el.text if source_el is not None else url

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
        results = await asyncio.gather(
            *[fetch_rss_feed(session, url) for url in RSS_FEEDS],
            return_exceptions=True,
        )

    all_items = []
    for r in results:
        if isinstance(r, list):
            all_items.extend(r)

    seen = set()
    unique = []
    for item in all_items:
        key = (item.title, item.link)
        if key not in seen:
            seen.add(key)
            unique.append(item)

    return unique


# =========================
# TELEGRAM SENDER
# =========================

async def send_message_with_retry(bot: Bot, chat_id: int, text: str) -> None:
    await asyncio.sleep(2)
    while True:
        try:
            await bot.send_message(chat_id=chat_id, text=text)
            break
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
        except (TimedOut, NetworkError):
            await asyncio.sleep(5)
        except Exception:
            break


# =========================
# MAIN LOOP
# =========================

async def process_news_cycle(bot: Bot, conn: aiosqlite.Connection):
    items = await fetch_all_feeds()
    items.sort(key=lambda x: x.published or datetime.utcnow())

    for item in items:
        if await is_duplicate(conn, item):
            continue

        category = classify_news(item)
        _, chat_id = category_to_header_and_chat_id(category)
        if chat_id == 0:
            continue

        msg = build_message(item, category)
        await send_message_with_retry(bot, chat_id, msg)
        await store_item(conn, item)


async def main():
    if not TELEGRAM_BOT_TOKEN:
        logger.error("Missing TELEGRAM_BOT_TOKEN")
        return

    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    conn = await init_db(DB_PATH)

    stop_event = asyncio.Event()

    def stop(*_):
        stop_event.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    while not stop_event.is_set():
        try:
            await process_news_cycle(bot, conn)
        except Exception as e:
            logger.exception("Cycle error: %s", e)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=POLL_SECONDS)
        except asyncio.TimeoutError:
            pass

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
