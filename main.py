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
# Configuration & constants
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

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "600"))  # default 10 minutes
SIM_THRESHOLD = float(os.getenv("SIM_THRESHOLD", "0.92"))  # 0–1 range

DB_PATH = os.getenv("DB_PATH", "news_bot.db")

# RSS feeds (UK-focused)
RSS_FEEDS = [
    # BBC
    "https://feeds.bbci.co.uk/news/uk/rss.xml",
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://feeds.bbci.co.uk/sport/rss.xml",
    # Sky News
    "https://feeds.skynews.com/feeds/rss/uk.xml",
    "https://feeds.skynews.com/feeds/rss/world.xml",
    # ONS (statistical bulletins)
    "https://www.ons.gov.uk/rss",
]

# How far back to check for fuzzy duplicates
FUZZY_LOOKBACK_MINUTES = 180  # last 3 hours


@dataclass
class NewsItem:
    title: str
    link: str
    source: str
    published: Optional[datetime]


# =========================
# Database & deduplication
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


async def is_duplicate(
    conn: aiosqlite.Connection,
    item: NewsItem,
    sim_threshold: float = SIM_THRESHOLD,
) -> bool:
    """Check exact hash and fuzzy similarity against recent titles."""
    h = compute_hash(item.title, item.link)

    # Exact hash check
    try:
        await conn.execute("SELECT 1 FROM news_items WHERE hash = ?;", (h,))
        cursor = await conn.execute("SELECT 1 FROM news_items WHERE hash = ?;", (h,))
        row = await cursor.fetchone()
        await cursor.close()
        if row:
            logger.debug("Duplicate by hash: %s", item.title)
            return True
    except Exception as e:
        logger.exception("Error checking hash duplicate: %s", e)

    # Fuzzy similarity check against recent titles
    cutoff_time = datetime.utcnow() - timedelta(minutes=FUZZY_LOOKBACK_MINUTES)
    try:
        cursor = await conn.execute(
            SELECT_RECENT_TITLES_SQL,
            (cutoff_time.isoformat(),),
        )
        rows = await cursor.fetchall()
        await cursor.close()
    except Exception as e:
        logger.exception("Error fetching recent titles for fuzzy dedup: %s", e)
        rows = []

    normalized_new = normalize_text(item.title)
    for (existing_title,) in rows:
        ratio = SequenceMatcher(
            None, normalized_new, normalize_text(existing_title)
        ).ratio()
        if ratio >= sim_threshold:
            logger.debug(
                "Duplicate by fuzzy match (%.3f): %s ~ %s",
                ratio,
                item.title,
                existing_title,
            )
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
    except aiosqlite.IntegrityError:
        # Hash already exists
        logger.debug("Item already stored (hash collision/duplicate): %s", item.title)
    except Exception as e:
        logger.exception("Error storing item: %s", e)


# =========================
# Classification
# =========================

MACRO_KEYWORDS = [
    "inflation",
    "cpi",
    "ppi",
    "gdp",
    "recession",
    "interest rate",
    "interest rates",
    "central bank",
    "bank of england",
    "boe",
    "monetary policy",
    "quantitative easing",
    "quantitative tightening",
    "bond yields",
    "yields",
    "gilts",
    "unemployment",
    "wages",
    "labour market",
    "labor market",
    "sterling",
    "gbp",
    "pound",
]

SPORTS_KEYWORDS = [
    "football",
    "premier league",
    "championship",
    "fa cup",
    "carabao cup",
    "goal",
    "goals",
    "match",
    "fixture",
    "injury",
    "injuries",
    "transfer",
    "transfers",
    "manager",
    "coach",
    "striker",
    "midfielder",
    "defender",
    "goalkeeper",
    "club",
    "team",
]

POLITICS_KEYWORDS = [
    "prime minister",
    "pm",
    "downing street",
    "parliament",
    "mp",
    "mps",
    "labour",
    "labour party",
    "conservative",
    "conservatives",
    "tory",
    "tories",
    "lib dem",
    "liberal democrat",
    "snp",
    "green party",
    "election",
    "by-election",
    "referendum",
    "government",
    "minister",
    "cabinet",
    "home office",
    "foreign office",
    "no 10",
    "no. 10",
]

CELEBRITY_KEYWORDS = [
    "celebrity",
    "royal",
    "royals",
    "prince",
    "princess",
    "duke",
    "duchess",
    "king",
    "queen",
    "actor",
    "actress",
    "singer",
    "musician",
    "tv star",
    "reality star",
]


class Category:
    BREAKING = "BREAKING"
    MACRO = "MACRO"
    SPORTS = "SPORTS"


def classify_news(item: NewsItem) -> Category:
    text = f"{item.title} {item.source}".lower()

    def contains_any(keywords: List[str]) -> bool:
        return any(k in text for k in keywords)

    is_sports = contains_any(SPORTS_KEYWORDS)
    is_macro = contains_any(MACRO_KEYWORDS)
    is_politics = contains_any(POLITICS_KEYWORDS)
    is_celebrity = contains_any(CELEBRITY_KEYWORDS)

    # Sports has priority if clearly sports-related
    if is_sports:
        return Category.SPORTS

    # Macro only if macro signals and not politics/celebrity
    if is_macro and not (is_politics or is_celebrity):
        return Category.MACRO

    # Default: Breaking
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
# Message formatting
# =========================

def build_message(item: NewsItem, category: Category) -> str:
    header, _ = category_to_header_and_chat_id(category)
    title_upper = item.title.upper().strip()

    # Very lightweight, generic insights—these can be refined later
    insights = []

    if category == Category.MACRO:
        insights.append("Signals potential shifts in the UK economic outlook.")
        insights.append("May influence markets, business planning, or household finances.")
        insights.append("Relevant for policymakers, investors, and UK consumers.")
    elif category == Category.SPORTS:
        insights.append("Reflects momentum and narrative in UK sport.")
        insights.append("May affect team morale, fan sentiment, or league dynamics.")
        insights.append("Relevant for clubs, broadcasters, and supporters.")
    else:  # BREAKING
        insights.append("Highlights a significant development affecting the UK.")
        insights.append("May shape public debate, policy, or daily life.")
        insights.append("Important for understanding the broader UK context.")

    # Ensure exactly 3 insights
    insights = insights[:3]

    insights_block = "\n".join(f"• {i}" for i in insights)

    # Source name
    source_name = item.source.strip() or "Unknown"

    # Hashtags: always #Category #UK
    hashtag = f"#{category.capitalize()}"
    message = (
        f"{header}\n\n"
        f"{title_upper}\n\n"
        f"Why it matters:\n"
        f"{insights_block}\n\n"
        f"Source: {source_name}\n\n"
        f"{hashtag} #UK"
    )
    return message


# =========================
# RSS fetching
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

    # Minimal RSS parsing without external feedparser dependency
    # We’ll use a very simple regex-free approach based on tags.
    # For production, you might prefer `feedparser`, but this keeps dependencies lean.
    from xml.etree import ElementTree as ET

    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        logger.warning("Failed to parse RSS feed %s: %s", url, e)
        return items

    channel = root.find("channel")
    if channel is None:
        # Some feeds use namespaces; try a generic approach
        channel = root

    for item in channel.findall("item"):
        title_el = item.find("title")
        link_el = item.find("link")
        pub_el = item.find("pubDate")

        if title_el is None or link_el is None:
            continue

        title = title_el.text or ""
        link = link_el.text or ""
        pub_date_raw = pub_el.text if pub_el is not None else None

        published = None
        if pub_date_raw:
            try:
                # Many RSS feeds use RFC822-like dates
                from email.utils import parsedate_to_datetime

                published = parsedate_to_datetime(pub_date_raw)
            except Exception:
                published = None

        # Source name from channel title if available
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
        tasks = [fetch_rss_feed(session, url) for url in RSS_FEEDS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    all_items: List[NewsItem] = []
    for res in results:
        if isinstance(res, Exception):
            logger.warning("Error in feed fetch task: %s", res)
            continue
        all_items.extend(res)

    # Deduplicate within this batch by (title, link) to avoid immediate duplicates
    seen = set()
    unique_items: List[NewsItem] = []
    for item in all_items:
        key = (item.title, item.link)
        if key in seen:
            continue
        seen.add(key)
        unique_items.append(item)

    return unique_items


# =========================
# Telegram sending
# =========================

async def send_message_with_retry(
    bot: Bot,
    chat_id: int,
    text: str,
    min_delay_seconds: float = 2.0,
) -> None:
    """Send a message with basic rate limiting and retry logic."""
    await asyncio.sleep(min_delay_seconds)
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
# Main worker loop
# =========================

async def process_news_cycle(bot: Bot, conn: aiosqlite.Connection) -> None:
    logger.info("Fetching RSS feeds...")
    items = await fetch_all_feeds()
    logger.info("Fetched %d items from feeds", len(items))

    # Sort by published time if available, else keep order
    items.sort(key=lambda x: x.published or datetime.utcnow())

    for item in items:
        if not item.title or not item.link:
            continue

        # Deduplication
        if await is_duplicate(conn, item):
            continue

        # Classification
        category = classify_news(item)
        header, chat_id = category_to_header_and_chat_id(category)

        if chat_id == 0:
            logger.warning(
                "Chat ID for category %s is not configured; skipping item: %s",
                category,
                item.title,
            )
            continue

        # Build and send message
        msg = build_message(item, category)
        logger.info("Posting to %s: %s", header, item.title)
        await send_message_with_retry(bot, chat_id, msg)

        # Store in DB after successful send attempt (even if send ultimately failed,
        # we still want to avoid reposting the same headline repeatedly)
        await store_item(conn, item)


async def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN is not set. Exiting.")
        return

    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    conn = await init_db(DB_PATH)

    stop_event = asyncio.Event()

    def handle_signal(sig, frame):
        logger.info("Received signal %s, shutting down gracefully...", sig)
        stop_event.set()

    # Handle termination signals for long-lived worker
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info("UK News Telegram bot started. Poll interval: %s seconds", POLL_SECONDS)

    try:
        while not stop_event.is_set():
            try:
                await process_news_cycle(bot, conn)
            except Exception as e:
                logger.exception("Error in processing cycle: %s", e)

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=POLL_SECONDS)
            except asyncio.TimeoutError:
                # Normal wake-up for next cycle
                pass
    finally:
        await conn.close()
        logger.info("Bot stopped.")


if __name__ == "__main__":
    asyncio.run(main())
