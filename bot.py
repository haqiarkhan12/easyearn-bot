import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =====================================
# CONFIG
# =====================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "1347546821"))
DATABASE_URL = os.getenv("DATABASE_URL", "")
BOT_USERNAME = os.getenv("BOT_USERNAME", "EasyEarnAppBot")
PAYMENT_CHANNEL = os.getenv("PAYMENT_CHANNEL", "@easyearnpayments")
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "@haqiarkhan12")
ADMIN_START_STARS = float(os.getenv("ADMIN_START_STARS", "10000"))

FORCE_JOIN_CHANNELS = [
    ("@easyearnofficial1222", "https://t.me/easyearnofficial1222"),
    ("@easyearnpayments", "https://t.me/easyearnpayments"),
    ("@easyearnu", "https://t.me/easyearnu"),
]

REFERRAL_PERCENT = 15
DAILY_BONUS_STARS = 1.0
WITHDRAW_OPTIONS = [15.0, 25.0, 50.0]
BONUS_INTERVAL_HOURS = 24
PROMO_INTERVAL_HOURS = 24
LEAVE_CHECK_INTERVAL_HOURS = 2

PROMO_TEXT = (
    "ðŸ“¢ Ø²Ù…ÙˆÙ†Ú– Ø®Ø¯Ù…Ø§Øª\n\n"
    "â­ Ø¯ ØªÙ„ÛŒÚ«Ø±Ø§Ù… Ù¾Ø±ÛŒÙ…ÛŒÙ… Ø§Ùˆ Ø³ØªÙˆØ±ÙŠ Ø§Ø®ÛŒØ³ØªÙ„\n"
    "ðŸ“¢ Ø¯ ØªÙ„ÛŒÚ«Ø±Ø§Ù… Ø§Ø¹Ù„Ø§Ù†\n"
    "ðŸ“˜ Ø¯ ÙÛŒØ³Ø¨ÙˆÚ© Ø§Ùˆ Ø§Ù†Ø³Ù¼Ø§Ú«Ø±Ø§Ù… Ø§Ø¹Ù„Ø§Ù†ÙˆÙ†Ù‡\n"
    "ðŸ“± Ø¯ Ø®Ø§Ø±Ø¬ÙŠ ÙˆÛŒØ±Ú†ÙˆÙ„ Ù†Ù…Ø¨Ø±ÙˆÙ†Ù‡ Ø§Ø®ÛŒØ³ØªÙ„\n\n"
    f"ðŸ“© Ø¯ ØªØ±Ù„Ø§Ø³Ù‡ Ú©ÙˆÙ„Ùˆ Ù„Ù¾Ø§Ø±Ù‡ Ù„Ø§Ù†Ø¯ÙŠ Ø¢ÙŠÚ‰ÙŠ ØªÙ‡ Ù…Ø³Ø¬ ÙˆÚ©Ú“Ø¦:\n{SUPPORT_USERNAME}"
)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is missing")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =====================================
# DB
# =====================================
def db_connect():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def execute(query: str, params: tuple = (), returning: bool = False):
    conn = db_connect()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query, params)
    result = cur.fetchone() if returning else None
    conn.commit()
    cur.close()
    conn.close()
    return dict(result) if result else None


def fetch_one(query: str, params: tuple = ()) -> Optional[dict]:
    conn = db_connect()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query, params)
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row) if row else None


def fetch_all(query: str, params: tuple = ()) -> list[dict]:
    conn = db_connect()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query, params)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def safe_exec(query: str):
    try:
        execute(query)
    except Exception as e:
        logger.info("safe exec skipped: %s", e)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_pretty(value: Optional[str] = None) -> str:
    try:
        dt = datetime.fromisoformat(value) if value else datetime.now()
        try:
            return dt.strftime("%-d %b %Y, %-I:%M:%S %p")
        except Exception:
            return dt.strftime("%d %b %Y, %I:%M:%S %p")
    except Exception:
        return "Unknown"


def hours_since(value: str) -> float:
    try:
        dt = datetime.fromisoformat(value)
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    except Exception:
        return 999999


def init_db():
    execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            lang TEXT DEFAULT 'ps',
            stars NUMERIC(12,2) DEFAULT 0,
            referrer_id BIGINT,
            last_bonus_at TEXT,
            created_at TEXT
        )
        """
    )

    execute(
        """
        CREATE TABLE IF NOT EXISTS tasks (
            id SERIAL PRIMARY KEY,
            channel_title TEXT NOT NULL,
            chat_username TEXT NOT NULL,
            link TEXT NOT NULL,
            reward_stars NUMERIC(12,2) DEFAULT 0.5,
            status TEXT DEFAULT 'active',
            created_at TEXT
        )
        """
    )

    execute(
        """
        CREATE TABLE IF NOT EXISTS user_tasks (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            task_id INTEGER NOT NULL,
            rewarded_stars NUMERIC(12,2) DEFAULT 0.5,
            reward_removed INTEGER DEFAULT 0,
            status TEXT DEFAULT 'completed',
            created_at TEXT,
            last_checked_at TEXT,
            UNIQUE(user_id, task_id)
        )
        """
    )

    execute(
        """
        CREATE TABLE IF NOT EXISTS withdrawals (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            amount NUMERIC(12,2) DEFAULT 0,
            amount_stars NUMERIC(12,2) DEFAULT 0,
            status TEXT DEFAULT 'pending',
            admin_message_id BIGINT,
            created_at TEXT,
            approved_at TEXT,
            rejected_at TEXT
        )
        """
    )

    execute(
        """
        CREATE TABLE IF NOT EXISTS promo_chats (
            chat_id BIGINT PRIMARY KEY,
            title TEXT,
            chat_type TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TEXT
        )
        """
    )

    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS full_name TEXT")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS lang TEXT DEFAULT 'ps'")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS stars NUMERIC(12,2) DEFAULT 0")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS referrer_id BIGINT")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_bonus_at TEXT")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TEXT")

    safe_exec("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS created_at TEXT")
    safe_exec("ALTER TABLE withdrawals ADD COLUMN IF NOT EXISTS amount NUMERIC(12,2) DEFAULT 0")
    safe_exec("ALTER TABLE withdrawals ADD COLUMN IF NOT EXISTS amount_stars NUMERIC(12,2) DEFAULT 0")
    safe_exec("ALTER TABLE withdrawals ALTER COLUMN amount DROP NOT NULL")
    safe_exec("ALTER TABLE withdrawals ALTER COLUMN amount SET DEFAULT 0")
    safe_exec("ALTER TABLE withdrawals ALTER COLUMN amount_stars DROP NOT NULL")
    safe_exec("ALTER TABLE withdrawals ALTER COLUMN amount_stars SET DEFAULT 0")
    safe_exec("ALTER TABLE withdrawals ADD COLUMN IF NOT EXISTS admin_message_id BIGINT")
    safe_exec("ALTER TABLE withdrawals ADD COLUMN IF NOT EXISTS channel_message_id BIGINT")
    safe_exec("ALTER TABLE withdrawals ADD COLUMN IF NOT EXISTS approved_at TEXT")
    safe_exec("ALTER TABLE withdrawals ADD COLUMN IF NOT EXISTS rejected_at TEXT")
    safe_exec("ALTER TABLE promo_chats ADD COLUMN IF NOT EXISTS is_active INTEGER DEFAULT 1")
    safe_exec("ALTER TABLE promo_chats ADD COLUMN IF NOT EXISTS created_at TEXT")

    safe_exec("CREATE INDEX IF NOT EXISTS idx_user_tasks_user ON user_tasks(user_id)")
    safe_exec("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")

    admin = fetch_one("SELECT * FROM users WHERE user_id = %s", (ADMIN_ID,))
    if not admin:
        execute(
            "INSERT INTO users (user_id, username, full_name, lang, stars, created_at) VALUES (%s, %s, %s, 'ps', %s, %s)",
            (ADMIN_ID, "admin", "Admin", ADMIN_START_STARS, now_iso()),
        )
    else:
        current_stars = float(admin.get("stars") or 0)
        if current_stars < ADMIN_START_STARS:
            execute("UPDATE users SET stars = %s WHERE user_id = %s", (ADMIN_START_STARS, ADMIN_ID))

# =====================================
# HELPERS
# =====================================
def is_private(update: Update) -> bool:
    return bool(update.effective_chat and update.effective_chat.type == "private")


def ensure_user(user_id: int, username: str | None, full_name: str | None) -> None:
    row = fetch_one("SELECT * FROM users WHERE user_id = %s", (int(user_id),))
    if not row:
        execute(
            "INSERT INTO users (user_id, username, full_name, created_at) VALUES (%s, %s, %s, %s)",
            (int(user_id), username or "", full_name or "", now_iso()),
        )
    else:
        execute(
            "UPDATE users SET username = %s, full_name = %s WHERE user_id = %s",
            (username or "", full_name or "", int(user_id)),
        )


def get_user(user_id: int) -> Optional[dict]:
    return fetch_one("SELECT * FROM users WHERE user_id = %s", (int(user_id),))


def get_lang(user_id: int) -> str:
    row = get_user(user_id)
    if not row:
        return "ps"
    lang = (row.get("lang") or "ps").strip().lower()
    return lang if lang in ("ps", "en") else "ps"


def set_lang(user_id: int, lang: str):
    execute("UPDATE users SET lang = %s WHERE user_id = %s", (lang, int(user_id)))


def get_stars(user_id: int) -> float:
    row = fetch_one("SELECT stars FROM users WHERE user_id = %s", (int(user_id),))
    return float(row["stars"]) if row and row.get("stars") is not None else 0.0


def add_stars(user_id: int, amount: float):
    execute("UPDATE users SET stars = COALESCE(stars, 0) + %s WHERE user_id = %s", (amount, int(user_id)))


def referral_link(user_id: int) -> str:
    return f"https://t.me/{BOT_USERNAME}?start=ref_{int(user_id)}"


def referral_count(user_id: int) -> int:
    row = fetch_one("SELECT COUNT(*) AS c FROM users WHERE referrer_id = %s", (int(user_id),))
    return int(row["c"]) if row else 0


def top_referrals(limit: int = 50) -> list[dict]:
    return fetch_all(
        "SELECT referrer_id, COUNT(*) AS refs FROM users WHERE referrer_id IS NOT NULL GROUP BY referrer_id ORDER BY refs DESC LIMIT %s",
        (limit,),
    )


def get_user_refs(user_id):
    return fetch_all(
        "SELECT user_id AS id, username FROM users WHERE referrer_id = %s",
        (user_id,),
    )


def get_task(task_id: int) -> Optional[dict]:
    return fetch_one("SELECT * FROM tasks WHERE id = %s", (int(task_id),))


def add_task(channel_title: str, chat_username: str, link: str, reward_stars: float):
    execute(
        "INSERT INTO tasks (channel_title, chat_username, link, reward_stars, status, created_at) VALUES (%s, %s, %s, %s, 'active', %s)",
        (channel_title, chat_username, link, reward_stars, now_iso()),
    )


def save_promo_chat(chat_id: int, title: str, chat_type: str):
    execute(
        """
        INSERT INTO promo_chats (chat_id, title, chat_type, is_active, created_at)
        VALUES (%s, %s, %s, 1, %s)
        ON CONFLICT (chat_id)
        DO UPDATE SET title = EXCLUDED.title, chat_type = EXCLUDED.chat_type, is_active = 1
        """,
        (int(chat_id), title, chat_type, now_iso()),
    )


def deactivate_promo_chat(chat_id: int):
    execute("UPDATE promo_chats SET is_active = 0 WHERE chat_id = %s", (int(chat_id),))


def extract_chat_username(link_or_username: str) -> Optional[str]:
    value = (link_or_username or "").strip()
    if value.startswith("@"):
        return value
    m = re.search(r"t\.me/([A-Za-z0-9_]{4,})", value)
    if m:
        return "@" + m.group(1)
    return None


def human_remaining(delta: timedelta) -> str:
    total_seconds = max(0, int(delta.total_seconds()))
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    return f"{hours}h {minutes}m"

# =====================================
# TEXTS
# =====================================
TEXTS = {
    "ps": {
        "choose_lang": "Ú˜Ø¨Ù‡ Ø§Ù†ØªØ®Ø§Ø¨ Ú©Ú“Ø¦:",
        "intro": "ÚšÙ‡ Ø±Ø§ØºÙ„Ø§Ø³Øª EasyEarn Bot ØªÙ‡",
        "force_join": "Ù…Ù‡Ø±Ø¨Ø§Ù†ÙŠ ÙˆÚ©Ú“Ø¦ Ù¼ÙˆÙ„ Ú†ÛŒÙ†Ù„ÙˆÙ†Ù‡ Ø¬ÙˆÛŒÙ† Ú©Ú“Ø¦:",
        "joined_btn": "âœ… Ø¬ÙˆÛŒÙ† Ù…Û Ú©Ú“Ù„",
        "join_failed": "Ø§ÙˆÙ„ Ù¼ÙˆÙ„ Ø§Ú“ÛŒÙ† Ú†ÛŒÙ†Ù„ÙˆÙ†Ù‡ Ø¬ÙˆÛŒÙ† Ú©Ú“Ø¦.",
        "my_stars": "â­ Ø³ØªØ§Ø³Ùˆ Ø³ØªÙˆØ±ÙŠ: {stars}",
        "referral": "ðŸ‘¥ Ø³ØªØ§Ø³Ùˆ Ø±ÛŒÙØ±Ù„ Ù„ÛŒÙ†Ú©:\n{link}\n\nØªØ§Ø³Ùˆ Ø¨Ù‡ Ø¯ Ø®Ù¾Ù„Ùˆ Ø±ÛŒÙØ±Ù„ÙˆÙ†Ùˆ Ù„Ù‡ Ø¹Ø§ÛŒØ¯ Ú…Ø®Ù‡ 15% ØªØ±Ù„Ø§Ø³Ù‡ Ú©ÙˆØ¦.\nØ¬Ø¹Ù„ÙŠ Ø±ÛŒÙØ±Ù„ Ù†Ù‡ Ù…Ù†Ù„ Ú©ÛŒÚ–ÙŠØŒ Ú©Ù‡ ÙˆÙ¾ÛŒÚ˜Ù†Ø¯Ù„ Ø´ÙŠ Ø³ØªØ§Ø³Ùˆ Ø§Ú©Ø§ÙˆÙ†Ù¼ Ø¨Ù‡ Ø¨Ù†Ø¯ Ø´ÙŠ.\n\nÙ¼ÙˆÙ„ Ø±ÛŒÙØ±Ù„ÙˆÙ†Ù‡: {count}",
        "tasks_empty": "âŒ ÙØ¹Ù„Ø§Ù‹ Ù‡ÛÚ… ØªØ§Ø³Ú© Ù†Ø´ØªÙ‡",
        "task_done": "âœ… ØªØ§Ø³Ú© Ø¨Ø´Ù¾Ú“ Ø´Ùˆ\nâ­ {stars}",
        "task_already": "ØªØ§Ø³Ùˆ Ø¯Ø§ ØªØ§Ø³Ú© Ù…Ø®Ú©Û Ø¨Ø´Ù¾Ú“ Ú©Ú“ÛŒ",
        "task_fail": "âŒ Ù„ÙˆÙ…Ú“ÛŒ Ú†ÛŒÙ†Ù„ Ø¬ÙˆÛŒÙ† Ú©Ú“Ù‡ØŒ Ø¨ÛŒØ§ ØªØ§ÛŒÛŒØ¯ ÙˆÚ©Ú“Ù‡",
        "bonus_added": "âœ… ÙˆØ±ÚÙ†ÛŒ Ø¨ÙˆÙ†Ø³ ÙˆØ§Ø®ÛŒØ³ØªÙ„ Ø´Ùˆ: {stars} â­",
        "bonus_wait": "â³ Ø¨ÙˆÙ†Ø³ Ù…Ø®Ú©Û Ø§Ø®ÛŒØ³ØªÙ„ Ø´ÙˆÛŒ. Ù¾Ø§ØªÛ ÙˆØ®Øª: {remaining}",
        "withdraw_choose": "ðŸ’¸ Ø¯ ÙˆÛŒÚ‰Ø±Ø§ Ù„Ù¾Ø§Ø±Ù‡ Ø§Ù†ØªØ®Ø§Ø¨ ÙˆÚ©Ú“Ø¦:",
        "withdraw_low": "âŒ Ø¨ÛŒÙ„Ø§Ù†Ø³ Ú©Ù… Ø¯ÛŒ",
        "admin_low": "âŒ Ø¯ Ø§Ú‰Ù…ÛŒÙ† Ø¨ÛŒÙ„Ø§Ù†Ø³ Ú©Ù… Ø¯ÛŒ",
        "about": "â„¹ï¸ Ø²Ù…ÙˆÙ†Ú– Ù¾Ù‡ Ø§Ú“Ù‡\n\nEasyEarn Bot Ø¯ ØªØ§Ø³Ú©ÙˆÙ†ÙˆØŒ Ø±ÛŒÙØ±Ù„ÙˆÙ†Ùˆ Ø§Ùˆ ÙˆØ±ÚÙ†ÙŠ Ø¨ÙˆÙ†Ø³ Ù„Ù‡ Ù„Ø§Ø±Û Ø¯ Ø³ØªÙˆØ±Ùˆ Ú«Ù¼Ù„Ùˆ Ø³ÛŒØ³ØªÙ… Ø¯ÛŒ.",
        "support": "ðŸ“ž Ø³Ù¾ÙˆØ±Ù¼\n\nÙ…Ù‡Ø±Ø¨Ø§Ù†ÙŠ ÙˆÚ©Ú“Ø¦ Ø¯Û ÛŒÙˆØ²Ø±Ù†ÛŒÙ… ØªÙ‡ Ù…Ø³Ø¬ ÙˆÚ©Ú“Ø¦:\n{username}",
        "new_task": "ðŸ“¢ Ù†ÙˆÛŒ ØªØ§Ø³Ú© Ø§Ø¶Ø§ÙÙ‡ Ø´Ùˆ!\nâ­ Ø§Ù†Ø¹Ø§Ù…: {reward}",
        "stats_admin": "ðŸ‘¥ Ù¼ÙˆÙ„ ÛŒÙˆØ²Ø±Ø§Ù†: {users}\nðŸ†• Ø¯ Ù†Ù† ÛŒÙˆØ²Ø±Ø§Ù†: {today}\nâ­ Ø¯ Ù¼ÙˆÙ„Ùˆ ÛŒÙˆØ²Ø±Ø§Ù†Ùˆ Ø³ØªÙˆØ±ÙŠ: {stars}\nâ­ Ø¯ Ø§Ú‰Ù…ÛŒÙ† Ø³ØªÙˆØ±ÙŠ: {admin_stars}\nðŸ“ ÙØ¹Ø§Ù„ ØªØ§Ø³Ú©ÙˆÙ†Ù‡: {tasks}",
        "admin_only": "Ø¯Ø§ Ø¨Ø±Ø®Ù‡ ÛŒÙˆØ§Ø²Û Ø§Ú‰Ù…ÛŒÙ† ØªÙ‡ Ø¯Ù‡.",
        "admin_help": "ðŸ›  Admin Commands\n\n/users\n/refstats\n/withdraws\n/botstats\n/taskslist\n/taskstats",
        "broadcast_prompt": "Ù‡ØºÙ‡ Ù…Ø³Ø¬ ÙˆÙ„ÛŒÚ©Ø¦ Ú†Û Ù¼ÙˆÙ„Ùˆ users ØªÙ‡ ÙˆÙ„Ø§Ú“ Ø´ÙŠ.",
        "addtask_link": "Ø¯ Ú†ÛŒÙ†Ù„ Ù„ÛŒÙ†Ú© ÛŒØ§ @username Ø±Ø§ÙˆÙ„ÛÚ–Ø¦.",
        "addtask_title": "Ø¯ Ú†ÛŒÙ†Ù„ Ø¹Ù†ÙˆØ§Ù† Ø±Ø§ÙˆÙ„ÛÚ–Ø¦.",
        "addtask_reward": "Ø±ÛŒÙˆØ§Ø±Ú‰ ÙˆÙ„ÛŒÚ©Ø¦ØŒ Ù…Ø«Ø§Ù„: 0.5",
        "addbalance_prompt": "Ù‡ØºÙ‡ stars ÙˆÙ„ÛŒÚ©Ø¦ Ú†Û Ø§Ú‰Ù…ÛŒÙ† Ø¨ÛŒÙ„Ø§Ù†Ø³ ØªÙ‡ Ø§Ø¶Ø§ÙÙ‡ Ø´ÙŠ. Ù…Ø«Ø§Ù„: 1000",
        "addbalance_done": "âœ… Ø§Ú‰Ù…ÛŒÙ† Ø¨ÛŒÙ„Ø§Ù†Ø³ {amount} stars Ø³Ø±Ù‡ Ø²ÛŒØ§Øª Ø´Ùˆ.\nâ­ Ù†ÙˆÛŒ Ø¨ÛŒÙ„Ø§Ù†Ø³: {new_balance}",
        "removetask_prompt": "Ø¯ Ù„Ø±Û Ú©ÙˆÙ„Ùˆ Ù„Ù¾Ø§Ø±Ù‡ ØªØ§Ø³Ú© Ø§Ù†ØªØ®Ø§Ø¨ Ú©Ú“Ø¦.",
        "cancelled": "âŒ Ø¹Ù…Ù„ Ù„ØºÙˆÙ‡ Ø´Ùˆ.",
        "open_task_btn": "ðŸ”— ØªØ§Ø³Ú© Ø®Ù„Ø§Øµ Ú©Ú“Ù‡",
        "verify_btn": "âœ… ØªØ§ÛŒÛŒØ¯",
        "task_item": "ðŸ“¢ {title}\nâ­ Ø§Ù†Ø¹Ø§Ù…: {stars}",
        "leave_notice": "âš ï¸ ØªØ§Ø³Ùˆ ÛŒÙˆ Ú†ÛŒÙ†Ù„ Ù¾Ø±ÛÚšÙˆØ¯. Ø³ØªØ§Ø³Ùˆ Ø±ÛŒÙˆØ§Ø±Ø¯ Ø¨ÛØ±ØªÙ‡ Ú©Ù… Ø´Ùˆ. Ù‡ÛŒÙ„Ù‡ Ø¯Ù‡ Ø¨ÛØ±ØªÙ‡ ÛŒÛ subscribe Ú©Ú“Ø¦ Ø§Ùˆ Ø®Ù¾Ù„ Ø¨ÛŒÙ„Ø§Ù†Ø³ Ø²ÛŒØ§Øª Ú©Ú“Ø¦.",
    },
    "en": {
        "choose_lang": "Choose language:",
        "intro": "Welcome to EasyEarn Bot",
        "force_join": "Please join all required channels first:",
        "joined_btn": "âœ… I Joined",
        "join_failed": "Please join all required channels first.",
        "my_stars": "â­ Your stars: {stars}",
        "referral": "ðŸ‘¥ Your referral link:\n{link}\n\nYou earn 15% from your referrals' earnings. Fake referrals are not accepted. If detected, your account may be banned.\n\nTotal referrals: {count}",
        "tasks_empty": "âŒ No tasks available right now.",
        "task_done": "âœ… Task completed\nâ­ {stars}",
        "task_already": "You already completed this task.",
        "task_fail": "âŒ Please join the channel first, then verify.",
        "bonus_added": "âœ… Daily bonus claimed: {stars} â­",
        "bonus_wait": "â³ Bonus already claimed. Remaining: {remaining}",
        "withdraw_choose": "ðŸ’¸ Choose your withdrawal option:",
        "withdraw_low": "âŒ Insufficient balance.",
        "admin_low": "âŒ Admin balance is low.",
        "about": "â„¹ï¸ About Us\n\nEasyEarn Bot is a stars earning system through tasks, referrals, and daily bonus.",
        "support": "ðŸ“ž Support\n\nPlease message:\n{username}",
        "new_task": "ðŸ“¢ New task added!\nâ­ Reward: {reward}",
        "stats_admin": "ðŸ‘¥ Total users: {users}\nðŸ†• Today users: {today}\nâ­ Total user stars: {stars}\nâ­ Admin stars: {admin_stars}\nðŸ“ Active tasks: {tasks}",
        "admin_only": "This section is admin only.",
        "admin_help": "ðŸ›  Admin Commands\n\n/users\n/refstats\n/withdraws\n/botstats\n/taskslist\n/taskstats",
        "broadcast_prompt": "Send the message you want to broadcast.",
        "addtask_link": "Send channel link or @username.",
        "addtask_title": "Send channel title.",
        "addtask_reward": "Send reward, example: 0.5",
        "addbalance_prompt": "Send stars amount to add to admin balance. Example: 1000",
        "addbalance_done": "âœ… Admin balance increased by {amount} stars.\nâ­ New balance: {new_balance}",
        "removetask_prompt": "Choose a task to remove.",
        "cancelled": "âŒ Action cancelled.",
        "open_task_btn": "ðŸ”— Open Task",
        "verify_btn": "âœ… Verify",
        "task_item": "ðŸ“¢ {title}\nâ­ Reward: {stars}",
        "leave_notice": "âš ï¸ You left a channel. Your reward was deducted. Please subscribe again and increase your balance.",
    },
}


def t(user_id: int, key: str, **kwargs) -> str:
    return TEXTS[get_lang(user_id)][key].format(**kwargs)

# =====================================
# UI
# =====================================
def main_menu(user_id: int):
    lang = get_lang(user_id)
    if lang == "ps":
        keyboard = [
            ["ðŸ§ Withdraw", "â­ My Stars"],
            ["ðŸ‘¥ Referral", "ðŸ“ Tasks"],
            ["ðŸŽ Bonus", "ðŸŒ Language"],
            ["â„¹ï¸ About Us", "ðŸ“ž Support"],
        ]
        if int(user_id) == ADMIN_ID:
            keyboard.insert(0, ["ðŸ“Š Statistics", "ðŸ“£ Broadcast"])
            keyboard.insert(1, ["ðŸ›  Add Task", "ðŸ—‘ Remove Task"])
            keyboard.insert(2, ["âž• Add Balance"])
    else:
        keyboard = [
            ["ðŸ§ Withdraw", "â­ My Stars"],
            ["ðŸ‘¥ Referral", "ðŸ“ Tasks"],
            ["ðŸŽ Bonus", "ðŸŒ Language"],
            ["â„¹ï¸ About Us", "ðŸ“ž Support"],
        ]
        if int(user_id) == ADMIN_ID:
            keyboard.insert(0, ["ðŸ“Š Statistics", "ðŸ“£ Broadcast"])
            keyboard.insert(1, ["ðŸ›  Add Task", "ðŸ—‘ Remove Task"])
            keyboard.insert(2, ["âž• Add Balance"])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def cancel_reply_keyboard(user_id: int):
    return ReplyKeyboardMarkup([["âŒ Cancel"]], resize_keyboard=True)


def lang_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("ðŸ‡¦ðŸ‡« Ù¾ÚšØªÙˆ", callback_data="lang_ps")],
        [InlineKeyboardButton("ðŸ‡¬ðŸ‡§ English", callback_data="lang_en")],
        [InlineKeyboardButton("â¬…ï¸ Back", callback_data="back_main")],
    ])


def force_join_keyboard(user_id: int):
    rows = []
    for username, link in FORCE_JOIN_CHANNELS:
        rows.append([InlineKeyboardButton(f"ðŸ“¢ {username}", url=link)])
    rows.append([InlineKeyboardButton(t(user_id, "joined_btn"), callback_data="check_force_join")])
    return InlineKeyboardMarkup(rows)


def task_keyboard(user_id: int, task_id: int, link: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(t(user_id, "open_task_btn"), url=link)],
        [InlineKeyboardButton(t(user_id, "verify_btn"), callback_data=f"verify_{task_id}")],
        [InlineKeyboardButton("â¬…ï¸ Back" if get_lang(user_id) != "ps" else "â¬…ï¸ Ø´Ø§ØªÙ‡", callback_data="back_main")],
    ])


def withdraw_keyboard(user_id: int):
    rows = []
    for amount in WITHDRAW_OPTIONS:
        rows.append([InlineKeyboardButton(f"â­ {amount:g} Stars", callback_data=f"withdraw_{amount}")])
    rows.append([InlineKeyboardButton("â¬…ï¸ Back" if get_lang(user_id) != "ps" else "â¬…ï¸ Ø´Ø§ØªÙ‡", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)

# =====================================
# PROMO / TRACKING
# =====================================
async def track_bot_chats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat or not update.my_chat_member:
        return
    status = update.my_chat_member.new_chat_member.status
    title = chat.title or chat.username or str(chat.id)
    if status in ("administrator", "member"):
        save_promo_chat(chat.id, title, chat.type)
    elif status in ("left", "kicked"):
        deactivate_promo_chat(chat.id)


async def daily_promo_post(context: ContextTypes.DEFAULT_TYPE):
    chats = fetch_all("SELECT chat_id FROM promo_chats WHERE is_active = 1")
    for row in chats:
        try:
            await context.bot.send_message(chat_id=row["chat_id"], text=PROMO_TEXT)
        except Exception as e:
            logger.info("promo failed for %s: %s", row["chat_id"], e)

# =====================================
# JOIN CHECKS / PENALTIES
# =====================================
async def check_join(bot, chat_username: str, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=chat_username, user_id=int(user_id))
        return member.status in ("member", "administrator", "creator", "owner")
    except Exception:
        return False


async def check_force_join_all(bot, user_id: int) -> bool:
    for username, _ in FORCE_JOIN_CHANNELS:
        if not await check_join(bot, username, user_id):
            return False
    return True


async def process_leave_penalties(bot, user_id: int):
    rows = fetch_all(
        """
        SELECT ut.id, ut.task_id, ut.rewarded_stars, t.chat_username
        FROM user_tasks ut
        JOIN tasks t ON ut.task_id = t.id
        WHERE ut.user_id = %s
          AND ut.status = 'completed'
          AND ut.reward_removed = 0
        """,
        (int(user_id),),
    )

    for row in rows:
        if await check_join(bot, row["chat_username"], user_id):
            execute(
                "UPDATE user_tasks SET last_checked_at = %s WHERE id = %s",
                (now_iso(), row["id"]),
            )
            continue

        add_stars(user_id, -float(row["rewarded_stars"]))

        execute(
            """
            UPDATE user_tasks
            SET reward_removed = 1,
                status = 'left',
                last_checked_at = %s
            WHERE id = %s
            """,
            (now_iso(), row["id"]),
        )

        try:
            await bot.send_message(
                chat_id=user_id,
                text=t(user_id, "leave_notice"),
            )
        except Exception:
            pass


async def periodic_leave_check(context: ContextTypes.DEFAULT_TYPE):
    for row in fetch_all("SELECT user_id FROM users"):
        try:
            await process_leave_penalties(context.bot, row["user_id"])
        except Exception as e:
            logger.info("leave check failed for %s: %s", row["user_id"], e)

# =====================================
# START / CALLBACKS
# =====================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return

    user = update.effective_user
    text = update.message.text or ""

    referrer_id = None
    if text.startswith("/start ref_"):
        try:
            referrer_id = int(text.split("ref_")[1].strip())
            if referrer_id == int(user.id):
                referrer_id = None
        except Exception:
            referrer_id = None

    ensure_user(int(user.id), user.username or "", user.full_name or "")

    current_user = get_user(int(user.id))
    if referrer_id and current_user and not current_user.get("referrer_id"):
        execute(
            "UPDATE users SET referrer_id = %s WHERE user_id = %s",
            (referrer_id, int(user.id)),
        )

    context.user_data.pop("admin_flow", None)

    if not await check_force_join_all(context.bot, int(user.id)):
        await update.message.reply_text(
            t(user.id, "force_join"),
            reply_markup=force_join_keyboard(int(user.id)),
        )
        return

    await update.message.reply_text(
        t(user.id, "intro"),
        reply_markup=main_menu(int(user.id)),
    )


async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user = update.effective_user
    data = query.data or ""

    if data == "check_force_join":
        if await check_force_join_all(context.bot, int(user.id)):
            await query.message.reply_text(
                "âœ… Access granted." if get_lang(int(user.id)) != "ps" else "âœ… Ù„Ø§Ø³Ø±Ø³ÛŒ Ø¯Ø±Ú©Ú“Ù„ Ø´Ùˆ",
                reply_markup=main_menu(int(user.id)),
            )
        else:
            await query.message.reply_text(
                "âŒ Please join all required channels first." if get_lang(int(user.id)) != "ps" else "âŒ Ù…Ù‡Ø±Ø¨Ø§Ù†ÙŠ ÙˆÚ©Ú“Ø¦ Ù„ÙˆÙ…Ú“ÛŒ Ù¼ÙˆÙ„ Ø§Ú“ÛŒÙ† Ú†ÛŒÙ†Ù„ÙˆÙ†Ù‡ Ø¬ÙˆÛŒÙ† Ú©Ú“Ø¦",
                reply_markup=main_menu(int(user.id)),
            )
        return

    if data == "back_main":
        await query.message.reply_text(
            t(user.id, "intro"),
            reply_markup=main_menu(int(user.id)),
        )
        return

    if data == "language":
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("ðŸ‡¬ðŸ‡§ English", callback_data="lang_en"),
                InlineKeyboardButton("ðŸ‡¦ðŸ‡« Ù¾ÚšØªÙˆ", callback_data="lang_ps"),
            ],
            [InlineKeyboardButton("â¬…ï¸ Back", callback_data="back_main")],
        ])
        await query.message.reply_text(
            "Choose Language:" if get_lang(int(user.id)) != "ps" else "Ú˜Ø¨Ù‡ ÙˆÙ¼Ø§Ú©Ø¦:",
            reply_markup=kb,
        )
        return

    if data == "lang_en":
        execute(
            "UPDATE users SET lang = %s WHERE user_id = %s",
            ("en", int(user.id)),
        )
        await query.message.reply_text(
            "âœ… Language changed to English",
            reply_markup=main_menu(int(user.id)),
        )
        return

    if data == "lang_ps":
        execute(
            "UPDATE users SET lang = %s WHERE user_id = %s",
            ("ps", int(user.id)),
        )
        await query.message.reply_text(
            "âœ… Ú˜Ø¨Ù‡ Ù¾ÚšØªÙˆ ØªÙ‡ Ø¨Ø¯Ù„Ù‡ Ø´ÙˆÙ‡",
            reply_markup=main_menu(int(user.id)),
        )
        return

    if data == "bonus":
        row = get_user(int(user.id))
        last_bonus = row.get("last_bonus_at") if row else None

        if last_bonus and hours_since(last_bonus) < BONUS_INTERVAL_HOURS:
            remain = BONUS_INTERVAL_HOURS - hours_since(last_bonus)
            await query.message.reply_text(
                t(user.id, "bonus_wait", remaining=f"{remain:.0f}h"),
                reply_markup=main_menu(int(user.id)),
            )
            return

        add_stars(int(user.id), DAILY_BONUS_STARS)
        execute(
            "UPDATE users SET last_bonus_at = %s WHERE user_id = %s",
            (now_iso(), int(user.id)),
        )
        await query.message.reply_text(
            t(user.id, "bonus_added", stars=f"{DAILY_BONUS_STARS:g}"),
            reply_markup=main_menu(int(user.id)),
        )
        return

    if data == "balance":
        stars = get_stars(int(user.id))
        await query.message.reply_text(
            t(user.id, "my_stars", stars=f"{stars:g}"),
            reply_markup=main_menu(int(user.id)),
        )
        return

    if data == "referral":
        me = await context.bot.get_me()
        link = f"https://t.me/{me.username}?start=ref_{user.id}"
        count = referral_count(int(user.id))
        await query.message.reply_text(
            t(user.id, "referral", link=link, count=count),
            reply_markup=main_menu(int(user.id)),
        )
        return

    if data == "tasks":
        rows = fetch_all(
            "SELECT * FROM tasks WHERE status = 'active' ORDER BY id DESC"
        )

        if not rows:
            await query.message.reply_text(
                t(user.id, "tasks_empty"),
                reply_markup=main_menu(int(user.id)),
            )
            return

        shown = 0

        for task in rows:
            done = fetch_one(
                """
                SELECT 1
                FROM user_tasks
                WHERE user_id = %s
                  AND task_id = %s
                  AND status = 'completed'
                  AND reward_removed = 0
                """,
                (int(user.id), task["id"]),
            )

            if done:
                continue

            await query.message.reply_text(
                t(user.id, "task_item", title=task["channel_title"], stars=f"{float(task['reward_stars']):g}"),
                reply_markup=task_keyboard(int(user.id), task["id"], task["link"]),
            )
            shown += 1

        if shown == 0:
            await query.message.reply_text(
                "âœ… You have completed all tasks" if get_lang(int(user.id)) != "ps" else "âœ… Ù¼ÙˆÙ„ ØªØ§Ø³Ú©ÙˆÙ†Ù‡ Ø¯Û Ø¨Ø´Ù¾Ú“ Ú©Ú“ÙŠ",
                reply_markup=main_menu(int(user.id)),
            )
        return

    if data.startswith("verify_"):
        task_id = int(data.split("_")[-1])

        task = fetch_one(
            "SELECT * FROM tasks WHERE id = %s AND status = 'active'",
            (task_id,),
        )
        if not task:
            await query.message.reply_text(
                "Task not found or inactive." if get_lang(int(user.id)) != "ps" else "ØªØ§Ø³Ú© ÙˆÙ†Ù‡ Ù…ÙˆÙ†Ø¯Ù„ Ø´Ùˆ ÛŒØ§ ØºÛŒØ±ÙØ¹Ø§Ù„ Ø¯ÛŒ",
                reply_markup=main_menu(int(user.id)),
            )
            return

        already = fetch_one(
            """
            SELECT 1
            FROM user_tasks
            WHERE user_id = %s AND task_id = %s
              AND status = 'completed'
              AND reward_removed = 0
            """,
            (int(user.id), task_id),
        )
        if already:
            await query.message.reply_text(
                t(user.id, "task_already"),
                reply_markup=main_menu(int(user.id)),
            )
            return

        if not await check_join(context.bot, task["chat_username"], int(user.id)):
            await query.message.reply_text(
                t(user.id, "task_fail"),
                reply_markup=main_menu(int(user.id)),
            )
            return

        reward = float(task["reward_stars"])

        if get_stars(ADMIN_ID) < reward:
            await query.message.reply_text(
                t(user.id, "admin_low"),
                reply_markup=main_menu(int(user.id)),
            )
            return

        add_stars(ADMIN_ID, -reward)
        add_stars(int(user.id), reward)

        execute(
            """
            INSERT INTO user_tasks
                (user_id, task_id, rewarded_stars, reward_removed, status, created_at, last_checked_at)
            VALUES (%s, %s, %s, 0, 'completed', %s, %s)
            """,
            (int(user.id), task_id, reward, now_iso(), now_iso()),
        )

        row = get_user(int(user.id))
        if row and row.get("referrer_id"):
            referral_bonus = round((reward * REFERRAL_PERCENT) / 100, 2)
            if referral_bonus > 0:
                add_stars(int(row["referrer_id"]), referral_bonus)

        await query.message.reply_text(
            t(user.id, "task_done", stars=f"{reward:g}"),
            reply_markup=main_menu(int(user.id)),
        )
        return

    if data.startswith("withdraw_"):
        amount = float(data.split("_")[-1])

        if get_stars(int(user.id)) < amount:
            await query.message.reply_text(
                t(user.id, "withdraw_low"),
                reply_markup=main_menu(int(user.id)),
            )
            return

        add_stars(int(user.id), -amount)

        wd = execute(
            """
            INSERT INTO withdrawals (user_id, amount, amount_stars, status, created_at)
            VALUES (%s, %s, %s, 'pending', %s)
            RETURNING id
            """,
            (int(user.id), amount, amount, now_iso()),
            returning=True,
        )

        wd_id = wd["id"]
        wd_user = get_user(int(user.id))
        username = f"@{wd_user['username']}" if wd_user and wd_user.get("username") else str(user.id)

        msg = (
            "ðŸ“¤ New Withdrawal Request!\n\n"
            f"ðŸ‘¤ User: {username}\n"
            f"ðŸ†” UserID: {user.id}\n"
            f"ðŸ’° Amount: {amount:g} â­\n"
            f"ðŸ•’ Time: {now_pretty()}\n"
            "â³ Status: Pending"
        )

        admin_kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("âœ… Approve", callback_data=f"admin_wd_ok_{wd_id}"),
                InlineKeyboardButton("âŒ Reject", callback_data=f"admin_wd_no_{wd_id}"),
            ]
        ])

        try:
            sent = await context.bot.send_message(ADMIN_ID, msg, reply_markup=admin_kb)
            execute(
                "UPDATE withdrawals SET admin_message_id = %s WHERE id = %s",
                (sent.message_id, wd_id),
            )
        except Exception:
            pass

        try:
            channel_sent = await context.bot.send_message(PAYMENT_CHANNEL, msg)
            execute(
                "UPDATE withdrawals SET channel_message_id = %s WHERE id = %s",
                (channel_sent.message_id, wd_id),
            )
        except Exception:
            pass

        await query.message.reply_text(
            f"âœ… Withdrawal request sent: {amount:g} â­" if get_lang(int(user.id)) != "ps" else f"âœ… Ø¯ ÙˆÛŒÚ‰Ø±Ø§ ØºÙˆÚšØªÙ†Ù‡ ÙˆØ§Ø³ØªÙˆÙ„ Ø´ÙˆÙ‡: {amount:g} â­",
            reply_markup=main_menu(int(user.id)),
        )
        return

    if data.startswith("admin_wd_ok_"):
        if int(user.id) != ADMIN_ID:
            return

        wd_id = int(data.split("_")[-1])
        wd = fetch_one("SELECT * FROM withdrawals WHERE id = %s", (wd_id,))
        if not wd or wd["status"] != "pending":
            return

        execute(
            "UPDATE withdrawals SET status = 'approved', approved_at = %s WHERE id = %s",
            (now_iso(), wd_id),
        )

        try:
            if wd.get("channel_message_id"):
                await context.bot.edit_message_text(
                    chat_id=PAYMENT_CHANNEL,
                    message_id=wd["channel_message_id"],
                    text=(
                        "ðŸ“¤ New Withdrawal Request!\n\n"
                        f"ðŸ‘¤ UserID: {wd['user_id']}\n"
                        f"ðŸ’° Amount: {float(wd['amount_stars']):g} â­\n"
                        f"ðŸ•’ Time: {now_pretty(wd.get('created_at'))}\n"
                        "âœ… Status: Approved"
                    )
                )
        except Exception:
            pass

        try:
            await context.bot.send_message(
                int(wd["user_id"]),
                f"âœ… Your withdrawal has been approved: {float(wd['amount_stars']):g} â­" if get_lang(int(wd["user_id"])) != "ps" else f"âœ… Ø³ØªØ§Ø³Ùˆ ÙˆÛŒÚ‰Ø±Ø§ Ù…Ù†Ø¸ÙˆØ± Ø´Ùˆ: {float(wd['amount_stars']):g} â­",
                reply_markup=main_menu(int(wd["user_id"])),
            )
        except Exception:
            pass

        await query.message.reply_text("âœ… Withdrawal approved.")
        return

    if data.startswith("admin_wd_no_"):
        if int(user.id) != ADMIN_ID:
            return

        wd_id = int(data.split("_")[-1])
        wd = fetch_one("SELECT * FROM withdrawals WHERE id = %s", (wd_id,))
        if not wd or wd["status"] != "pending":
            return

        add_stars(int(wd["user_id"]), float(wd["amount_stars"]))

        execute(
            "UPDATE withdrawals SET status = 'rejected', rejected_at = %s WHERE id = %s",
            (now_iso(), wd_id),
        )

        try:
            if wd.get("channel_message_id"):
                await context.bot.edit_message_text(
                    chat_id=PAYMENT_CHANNEL,
                    message_id=wd["channel_message_id"],
                    text=(
                        "ðŸ“¤ New Withdrawal Request!\n\n"
                        f"ðŸ‘¤ UserID: {wd['user_id']}\n"
                        f"ðŸ’° Amount: {float(wd['amount_stars']):g} â­\n"
                        f"ðŸ•’ Time: {now_pretty(wd.get('created_at'))}\n"
                        "âŒ Status: Rejected"
                    )
                )
        except Exception:
            pass

        try:
            await context.bot.send_message(
                int(wd["user_id"]),
                f"âŒ Your withdrawal has been rejected: {float(wd['amount_stars']):g} â­" if get_lang(int(wd["user_id"])) != "ps" else f"âŒ Ø³ØªØ§Ø³Ùˆ ÙˆÛŒÚ‰Ø±Ø§ Ø±Ø¯ Ø´Ùˆ: {float(wd['amount_stars']):g} â­",
                reply_markup=main_menu(int(wd["user_id"])),
            )
        except Exception:
            pass

        await query.message.reply_text("âŒ Withdrawal rejected.")
        return

# =====================================
# USER ROUTER
# =====================================
async def user_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return

    user = update.effective_user
    ensure_user(int(user.id), user.username or "", user.full_name or "")

    if not await check_force_join_all(context.bot, int(user.id)):
        await update.message.reply_text(
            t(user.id, "force_join"),
            reply_markup=force_join_keyboard(int(user.id)),
        )
        return

    await process_leave_penalties(context.bot, int(user.id))

    text = (update.message.text or "").strip()

    if text == "ðŸ“Š Statistics":
        if user.id != ADMIN_ID:
            await update.message.reply_text(t(user.id, "admin_only"), reply_markup=main_menu(user.id))
            return
        total_users = fetch_one("SELECT COUNT(*) AS c FROM users")
        today_users = fetch_one("SELECT COUNT(*) AS c FROM users WHERE created_at::date = CURRENT_DATE")
        total_stars = fetch_one("SELECT COALESCE(SUM(stars),0) AS s FROM users")
        active_tasks = fetch_one("SELECT COUNT(*) AS c FROM tasks WHERE status = 'active'")
        await update.message.reply_text(
            t(
                user.id,
                "stats_admin",
                users=int(total_users["c"]) if total_users else 0,
                today=int(today_users["c"]) if today_users else 0,
                stars=f"{float(total_stars['s']) if total_stars else 0:g}",
                admin_stars=f"{get_stars(ADMIN_ID):g}",
                tasks=int(active_tasks["c"]) if active_tasks else 0,
            ),
            reply_markup=main_menu(user.id),
        )
        return

    if text == "ðŸ“£ Broadcast":
        if user.id != ADMIN_ID:
            await update.message.reply_text(t(user.id, "admin_only"), reply_markup=main_menu(user.id))
            return
        context.user_data["admin_flow"] = "broadcast"
        await update.message.reply_text(t(user.id, "broadcast_prompt"), reply_markup=cancel_reply_keyboard(user.id))
        return

    if text == "ðŸ›  Add Task":
        if user.id != ADMIN_ID:
            await update.message.reply_text(t(user.id, "admin_only"), reply_markup=main_menu(user.id))
            return
        context.user_data["admin_flow"] = "addtask_link"
        await update.message.reply_text(t(user.id, "addtask_link"), reply_markup=cancel_reply_keyboard(user.id))
        return

    if text == "ðŸ—‘ Remove Task":
        if user.id != ADMIN_ID:
            await update.message.reply_text(t(user.id, "admin_only"), reply_markup=main_menu(user.id))
            return

        tasks = fetch_all("SELECT id, channel_title FROM tasks WHERE status = 'active' ORDER BY id DESC")
        if not tasks:
            await update.message.reply_text("No active tasks.", reply_markup=main_menu(user.id))
            return

        buttons_list = []
        for task in tasks:
            buttons_list.append([InlineKeyboardButton(f"#{task['id']} - {task['channel_title']}", callback_data=f"remove_task_{task['id']}")])
        buttons_list.append([InlineKeyboardButton("â¬…ï¸ Back", callback_data="back_main")])

        await update.message.reply_text(
            t(user.id, "removetask_prompt"),
            reply_markup=InlineKeyboardMarkup(buttons_list),
        )
        return

    if text == "âž• Add Balance":
        if user.id != ADMIN_ID:
            await update.message.reply_text(t(user.id, "admin_only"), reply_markup=main_menu(user.id))
            return
        context.user_data["admin_flow"] = "addbalance"
        await update.message.reply_text(t(user.id, "addbalance_prompt"), reply_markup=cancel_reply_keyboard(user.id))
        return

    if text == "ðŸŒ Language":
        await update.message.reply_text(t(user.id, "choose_lang"), reply_markup=lang_keyboard())
        return

    if text == "â­ My Stars":
        await update.message.reply_text(t(user.id, "my_stars", stars=f"{get_stars(user.id):g}"), reply_markup=main_menu(user.id))
        return

    if text == "ðŸ‘¥ Referral":
        await update.message.reply_text(
            t(user.id, "referral", link=referral_link(user.id), count=referral_count(user.id)),
            reply_markup=main_menu(user.id),
        )
        return

    if text == "ðŸ“ Tasks":
        rows = fetch_all("SELECT * FROM tasks WHERE status = 'active' ORDER BY id DESC")

        if not rows:
            await update.message.reply_text(
                t(user.id, "tasks_empty"),
                reply_markup=main_menu(user.id),
            )
            return

        shown = 0
        for task in rows:
            done = fetch_one(
                """
                SELECT 1
                FROM user_tasks
                WHERE user_id = %s
                  AND task_id = %s
                  AND status = 'completed'
                  AND reward_removed = 0
                """,
                (int(user.id), task["id"]),
            )

            if done:
                continue

            await update.message.reply_text(
                t(user.id, "task_item", title=task["channel_title"], stars=f"{float(task['reward_stars']):g}"),
                reply_markup=task_keyboard(int(user.id), task["id"], task["link"]),
            )
            shown += 1

        if shown == 0:
            await update.message.reply_text(
                "âœ… You have completed all tasks" if get_lang(int(user.id)) != "ps" else "âœ… Ù¼ÙˆÙ„ ØªØ§Ø³Ú©ÙˆÙ†Ù‡ Ø¯Û Ø¨Ø´Ù¾Ú“ Ú©Ú“ÙŠ",
                reply_markup=main_menu(user.id),
            )
        return

    if text == "ðŸŽ Bonus":
        row = get_user(user.id)
        last_bonus = row.get("last_bonus_at") if row else None
        if last_bonus:
            try:
                last_dt = datetime.fromisoformat(last_bonus)
                next_dt = last_dt + timedelta(hours=BONUS_INTERVAL_HOURS)
                if datetime.now(timezone.utc) < next_dt:
                    await update.message.reply_text(
                        t(user.id, "bonus_wait", remaining=human_remaining(next_dt - datetime.now(timezone.utc))),
                        reply_markup=main_menu(user.id),
                    )
                    return
            except Exception:
                pass
        add_stars(user.id, DAILY_BONUS_STARS)
        execute("UPDATE users SET last_bonus_at = %s WHERE user_id = %s", (now_iso(), int(user.id)))
        await update.message.reply_text(t(user.id, "bonus_added", stars=f"{DAILY_BONUS_STARS:g}"), reply_markup=main_menu(user.id))
        return

    if text == "ðŸ§ Withdraw":
        await update.message.reply_text(t(user.id, "withdraw_choose"), reply_markup=withdraw_keyboard(user.id))
        return

    if text == "â„¹ï¸ About Us":
        await update.message.reply_text(t(user.id, "about"), reply_markup=main_menu(user.id))
        return

    if text == "ðŸ“ž Support":
        await update.message.reply_text(t(user.id, "support", username=SUPPORT_USERNAME), reply_markup=main_menu(user.id))
        return

    await update.message.reply_text(t(user.id, "intro"), reply_markup=main_menu(user.id))

# =====================================
# ADMIN COMMANDS / FLOWS
# =====================================
async def admin_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return
    await update.message.reply_text(t(update.effective_user.id, "admin_help"))


async def admin_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return
    rows = fetch_all("SELECT user_id, username, stars, referrer_id FROM users ORDER BY created_at DESC LIMIT 100")
    lines = []
    for r in rows:
        ref_by = r.get("referrer_id") or "-"
        lines.append(f"{r['user_id']} | @{r['username'] or 'no_username'} | â­ {float(r['stars'] or 0):g} | ref_by: {ref_by}")
    await update.message.reply_text("\n".join(lines) or "No users")


async def admin_refstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return

    rows = top_referrals(50)
    if not rows:
        await update.message.reply_text("No referrals yet")
        return

    lines = []
    for i, row in enumerate(rows, start=1):
        u = get_user(row["referrer_id"])
        username = f"@{u['username']}" if u and u.get("username") else str(row["referrer_id"])

        refs = get_user_refs(row["referrer_id"])
        ref_list = ", ".join(
            [f"@{x['username']}" if x.get("username") else str(x.get("id")) for x in refs]
        ) if refs else "No refs"

        lines.append(f"{i}. {username} - {len(refs)} refs\nðŸ‘‰ {ref_list}")

    await update.message.reply_text("ðŸ† Referral Leaderboard\n\n" + "\n\n".join(lines))


async def admin_withdraws(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return
    rows = fetch_all("SELECT * FROM withdrawals WHERE status = 'pending' ORDER BY created_at DESC LIMIT 50")
    text = "\n".join([f"#{r['id']} | User {r['user_id']} | â­ {float(r['amount_stars']):g} | {r['status']}" for r in rows]) or "No pending withdraws"
    await update.message.reply_text(text)


async def admin_botstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return
    total_users = fetch_one("SELECT COUNT(*) AS c FROM users")
    total_tasks = fetch_one("SELECT COUNT(*) AS c FROM tasks WHERE status = 'active'")
    total_withdraws = fetch_one("SELECT COUNT(*) AS c FROM withdrawals")
    total_stars = fetch_one("SELECT COALESCE(SUM(stars),0) AS s FROM users")
    await update.message.reply_text(
        "ðŸ“Š Bot Stats\n\n"
        f"Users: {int(total_users['c']) if total_users else 0}\n"
        f"Active Tasks: {int(total_tasks['c']) if total_tasks else 0}\n"
        f"Withdraw Requests: {int(total_withdraws['c']) if total_withdraws else 0}\n"
        f"Total User Stars: {float(total_stars['s']) if total_stars else 0:g}\n"
        f"Admin Stars: {get_stars(ADMIN_ID):g}"
    )


async def admin_taskslist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return
    rows = fetch_all("SELECT id, channel_title, reward_stars, status FROM tasks ORDER BY id DESC LIMIT 100")
    text = "\n".join([f"#{r['id']} | {r['channel_title']} | â­ {float(r['reward_stars']):g} | {r['status']}" for r in rows]) or "No tasks"
    await update.message.reply_text(text)


async def admin_taskstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return
    rows = fetch_all(
        """
        SELECT
            t.id,
            t.channel_title,
            t.status,
            t.created_at,
            COUNT(ut.id) AS joined_count
        FROM tasks t
        LEFT JOIN user_tasks ut
            ON ut.task_id = t.id
           AND ut.reward_removed = 0
        WHERE t.status = 'active'
        GROUP BY t.id, t.channel_title, t.status, t.created_at
        ORDER BY t.id DESC
        LIMIT 100
        """
    )
    lines = []
    for r in rows:
        created = r.get("created_at")
        days = 0
        if created:
            try:
                days = (datetime.now(timezone.utc) - datetime.fromisoformat(created)).days
            except Exception:
                days = 0
        lines.append(f"#{r['id']} | {r['channel_title']} | joins: {r['joined_count']} | days: {days}")
    await update.message.reply_text("\n".join(lines) or "No task stats")


async def admin_flow_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not is_private(update) or update.effective_user.id != ADMIN_ID:
        return False

    flow = context.user_data.get("admin_flow")
    if not flow:
        return False

    text = (update.message.text or "").strip()

    if text.lower() in ("cancel", "/cancel", "âŒ cancel", "back", "â¬…ï¸ back"):
        context.user_data.pop("admin_flow", None)
        await update.message.reply_text(t(update.effective_user.id, "cancelled"), reply_markup=main_menu(update.effective_user.id))
        return True

    if flow == "broadcast":
        users = fetch_all("SELECT user_id FROM users")
        sent = 0
        failed = 0
        for u in users:
            try:
                await context.bot.send_message(chat_id=u["user_id"], text=text)
                sent += 1
            except Exception:
                failed += 1
        context.user_data.pop("admin_flow", None)
        await update.message.reply_text(f"âœ… Sent: {sent}\nâŒ Failed: {failed}", reply_markup=main_menu(update.effective_user.id))
        return True

    if flow == "addtask_link":
        username = extract_chat_username(text)
        if not username:
            await update.message.reply_text("Invalid link. Send public link or @username", reply_markup=cancel_reply_keyboard(update.effective_user.id))
            return True
        context.user_data["task_chat_username"] = username
        context.user_data["task_link"] = text if text.startswith("http") else f"https://t.me/{username[1:]}"
        context.user_data["admin_flow"] = "addtask_title"
        await update.message.reply_text(t(update.effective_user.id, "addtask_title"), reply_markup=cancel_reply_keyboard(update.effective_user.id))
        return True

    if flow == "addtask_title":
        context.user_data["task_title"] = text
        context.user_data["admin_flow"] = "addtask_reward"
        await update.message.reply_text(t(update.effective_user.id, "addtask_reward"), reply_markup=cancel_reply_keyboard(update.effective_user.id))
        return True

    if flow == "addtask_reward":
        try:
            reward = float(text)
        except Exception:
            await update.message.reply_text("Invalid reward. Example: 0.5", reply_markup=cancel_reply_keyboard(update.effective_user.id))
            return True
        add_task(context.user_data["task_title"], context.user_data["task_chat_username"], context.user_data["task_link"], reward)
        context.user_data.pop("admin_flow", None)
        await update.message.reply_text("âœ… Task added", reply_markup=main_menu(update.effective_user.id))
        return True

    if flow == "addbalance":
        try:
            amount = float(text)
        except Exception:
            await update.message.reply_text("Invalid amount. Example: 1000", reply_markup=cancel_reply_keyboard(update.effective_user.id))
            return True
        add_stars(ADMIN_ID, amount)
        context.user_data.pop("admin_flow", None)
        await update.message.reply_text(
            t(update.effective_user.id, "addbalance_done", amount=f"{amount:g}", new_balance=f"{get_stars(ADMIN_ID):g}"),
            reply_markup=main_menu(update.effective_user.id),
        )
        return True

    return False

# =====================================
# REMOVE TASK CALLBACK
# =====================================
async def remove_task_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    user = update.effective_user
    if not data.startswith("remove_task_"):
        return
    if user.id != ADMIN_ID:
        await query.answer("Admin only", show_alert=True)
        return

    task_id = int(data.split("_")[-1])
    execute("UPDATE tasks SET status = 'removed' WHERE id = %s", (task_id,))
    await query.answer("Task removed")
    await query.message.reply_text(f"âœ… Task #{task_id} removed", reply_markup=main_menu(user.id))

# =====================================
# MAIN
# =====================================
def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_help))
    app.add_handler(CommandHandler("users", admin_users))
    app.add_handler(CommandHandler("refstats", admin_refstats))
    app.add_handler(CommandHandler("withdraws", admin_withdraws))
    app.add_handler(CommandHandler("botstats", admin_botstats))
    app.add_handler(CommandHandler("taskslist", admin_taskslist))
    app.add_handler(CommandHandler("taskstats", admin_taskstats))

    app.add_handler(CallbackQueryHandler(remove_task_callback, pattern=r"^remove_task_\d+$"))
    app.add_handler(CallbackQueryHandler(buttons))
    app.add_handler(ChatMemberHandler(track_bot_chats, ChatMemberHandler.MY_CHAT_MEMBER))

    async def combined_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
        handled = await admin_flow_router(update, context)
        if handled:
            return
        await user_router(update, context)

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, combined_router))

    if app.job_queue:
        app.job_queue.run_repeating(periodic_leave_check, interval=LEAVE_CHECK_INTERVAL_HOURS * 3600, first=600)
        app.job_queue.run_repeating(daily_promo_post, interval=PROMO_INTERVAL_HOURS * 3600, first=900)

    logger.info("EasyEarn final bot is running...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
