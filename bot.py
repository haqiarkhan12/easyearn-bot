import logging
import os
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
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
ADMIN_START_STARS = Decimal(os.getenv("ADMIN_START_STARS", "10000"))
FORCE_JOIN_CHANNELS = [
    ("@easyearnofficial1222", "https://t.me/easyearnofficial1222"),
    ("@easyearnpayments", "https://t.me/easyearnpayments"),
    ("@easyearnu", "https://t.me/easyearnu"),
]
REFERRAL_PERCENT = Decimal("15")
DAILY_BONUS_STARS = Decimal("1.0")
WITHDRAW_OPTIONS = [Decimal("15.0"), Decimal("25.0"), Decimal("50.0")]
WITHDRAW_COOLDOWN_HOURS = 4
BONUS_INTERVAL_HOURS = 24
PROMO_INTERVAL_HOURS = 24
LEAVE_CHECK_INTERVAL_HOURS = 2
PROOF_TASK_TYPES = {"reaction", "bot_link", "youtube", "facebook"}
PROMO_TEXT = (
    "📢 زمونږ خدمات\n\n"
    "⭐ د تلیګرام پریمیم او ستوري اخیستل\n"
    "📢 د تلیګرام اعلان\n"
    "📘 د فیسبوک او انسټاګرام اعلانونه\n"
    "📱 د خارجي ویرچول نمبرونه اخیستل\n\n"
    f"📩 د ترلاسه کولو لپاره لاندي آيډي ته مسج وکړئ:\n{SUPPORT_USERNAME}"
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
    try:
        cur.execute(query, params)
        result = cur.fetchone() if returning else None
        conn.commit()
        return dict(result) if result else None
    finally:
        cur.close()
        conn.close()


def execute_many(statements: list[tuple[str, tuple]]):
    conn = db_connect()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        for query, params in statements:
            cur.execute(query, params)
        conn.commit()
    finally:
        cur.close()
        conn.close()


def transaction(callback):
    conn = db_connect()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        result = callback(conn, cur)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def fetch_one(query: str, params: tuple = ()) -> Optional[dict]:
    conn = db_connect()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(query, params)
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        cur.close()
        conn.close()


def fetch_all(query: str, params: tuple = ()) -> list[dict]:
    conn = db_connect()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(query, params)
        rows = [dict(r) for r in cur.fetchall()]
        return rows
    finally:
        cur.close()
        conn.close()


def safe_exec(query: str):
    try:
        execute(query)
    except Exception as e:
        logger.info("safe exec skipped: %s", e)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().isoformat()


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def now_pretty(value: Optional[str] = None) -> str:
    dt = parse_dt(value) if value else now_utc()
    if not dt:
        return "Unknown"
    try:
        return dt.astimezone().strftime("%-d %b %Y, %-I:%M:%S %p")
    except Exception:
        return dt.astimezone().strftime("%d %b %Y, %I:%M:%S %p")


def hours_since(value: Optional[str]) -> float:
    dt = parse_dt(value)
    if not dt:
        return 999999
    return (now_utc() - dt).total_seconds() / 3600.0


def decimalize(value) -> Decimal:
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0.00")


def pretty_amount(value) -> str:
    d = decimalize(value)
    s = format(d.normalize(), "f") if d != d.to_integral() else str(d.quantize(Decimal("1")))
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


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
            referral_paid INTEGER DEFAULT 0,
            last_bonus_at TEXT,
            created_at TEXT,
            is_banned INTEGER DEFAULT 0,
            ban_reason TEXT,
            banned_at TEXT,
            withdraw_eligible_at TEXT
        )
        """
    )
    execute(
        """
        CREATE TABLE IF NOT EXISTS tasks (
            id SERIAL PRIMARY KEY,
            task_type TEXT DEFAULT 'channel',
            channel_title TEXT NOT NULL,
            chat_username TEXT,
            link TEXT NOT NULL,
            reward_stars NUMERIC(12,2) DEFAULT 0.5,
            status TEXT DEFAULT 'active',
            created_at TEXT,
            requires_proof BOOLEAN DEFAULT FALSE,
            proof_required BOOLEAN DEFAULT FALSE,
            post_link TEXT,
            bot_link TEXT
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
            proof_file_id TEXT,
            proof_file_unique_id TEXT,
            proof_message_id BIGINT,
            reviewed_at TEXT,
            reviewed_by BIGINT,
            rejection_reason TEXT,
            suspicious INTEGER DEFAULT 0,
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
            channel_message_id BIGINT,
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
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_paid INTEGER DEFAULT 0")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_bonus_at TEXT")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TEXT")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS is_banned INTEGER DEFAULT 0")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS ban_reason TEXT")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS banned_at TEXT")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS withdraw_eligible_at TEXT")

    safe_exec("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS task_type TEXT DEFAULT 'channel'")
    safe_exec("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS chat_username TEXT")
    safe_exec("ALTER TABLE tasks ALTER COLUMN chat_username DROP NOT NULL")
    safe_exec("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS created_at TEXT")
    safe_exec("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS requires_proof BOOLEAN DEFAULT FALSE")
    safe_exec("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS proof_required BOOLEAN DEFAULT FALSE")
    safe_exec("UPDATE tasks SET task_type = 'channel' WHERE task_type IS NULL OR task_type = ''")
    safe_exec("UPDATE tasks SET proof_required = COALESCE(requires_proof, FALSE) WHERE proof_required IS NULL")
    safe_exec("ALTER TABLE tasks ALTER COLUMN proof_required SET DEFAULT FALSE")
    safe_exec("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS post_link TEXT")
    safe_exec("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS bot_link TEXT")

    safe_exec("ALTER TABLE user_tasks ADD COLUMN IF NOT EXISTS reward_removed INTEGER DEFAULT 0")
    safe_exec("ALTER TABLE user_tasks ADD COLUMN IF NOT EXISTS last_checked_at TEXT")
    safe_exec("ALTER TABLE user_tasks ADD COLUMN IF NOT EXISTS proof_file_id TEXT")
    safe_exec("ALTER TABLE user_tasks ADD COLUMN IF NOT EXISTS proof_file_unique_id TEXT")
    safe_exec("ALTER TABLE user_tasks ADD COLUMN IF NOT EXISTS proof_message_id BIGINT")
    safe_exec("ALTER TABLE user_tasks ADD COLUMN IF NOT EXISTS reviewed_at TEXT")
    safe_exec("ALTER TABLE user_tasks ADD COLUMN IF NOT EXISTS reviewed_by BIGINT")
    safe_exec("ALTER TABLE user_tasks ADD COLUMN IF NOT EXISTS rejection_reason TEXT")
    safe_exec("ALTER TABLE user_tasks ADD COLUMN IF NOT EXISTS suspicious INTEGER DEFAULT 0")

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

    safe_exec("CREATE INDEX IF NOT EXISTS idx_users_referrer ON users(referrer_id)")
    safe_exec("CREATE INDEX IF NOT EXISTS idx_users_banned ON users(is_banned)")
    safe_exec("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")
    safe_exec("CREATE INDEX IF NOT EXISTS idx_tasks_type ON tasks(task_type)")
    safe_exec("CREATE INDEX IF NOT EXISTS idx_user_tasks_user ON user_tasks(user_id)")
    safe_exec("CREATE INDEX IF NOT EXISTS idx_user_tasks_task ON user_tasks(task_id)")
    safe_exec("CREATE INDEX IF NOT EXISTS idx_user_tasks_status ON user_tasks(status)")
    safe_exec("CREATE INDEX IF NOT EXISTS idx_withdrawals_status ON withdrawals(status)")

    admin = fetch_one("SELECT * FROM users WHERE user_id = %s", (ADMIN_ID,))
    if not admin:
        execute(
            "INSERT INTO users (user_id, username, full_name, lang, stars, created_at) VALUES (%s, %s, %s, 'ps', %s, %s)",
            (ADMIN_ID, "admin", "Admin", ADMIN_START_STARS, now_iso()),
        )
    else:
        current_stars = decimalize(admin.get("stars") or 0)
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


def is_banned(user_id: int) -> bool:
    row = fetch_one("SELECT is_banned FROM users WHERE user_id = %s", (int(user_id),))
    return bool(row and int(row.get("is_banned") or 0) == 1)


def ban_user(user_id: int, reason: str = ""):
    execute(
        "UPDATE users SET is_banned = 1, ban_reason = %s, banned_at = %s WHERE user_id = %s",
        (reason or "", now_iso(), int(user_id)),
    )


def unban_user(user_id: int):
    execute(
        "UPDATE users SET is_banned = 0, ban_reason = NULL, banned_at = NULL WHERE user_id = %s",
        (int(user_id),),
    )


def get_stars(user_id: int) -> Decimal:
    row = fetch_one("SELECT stars FROM users WHERE user_id = %s", (int(user_id),))
    return decimalize(row["stars"]) if row and row.get("stars") is not None else Decimal("0.00")


def add_stars(user_id: int, amount: Decimal):
    amount = decimalize(amount)
    transaction(
        lambda conn, cur: cur.execute(
            "UPDATE users SET stars = COALESCE(stars, 0) + %s WHERE user_id = %s",
            (amount, int(user_id)),
        )
    )
    update_withdraw_eligibility_if_needed(int(user_id))


def subtract_stars(user_id: int, amount: Decimal):
    amount = decimalize(amount)
    transaction(
        lambda conn, cur: cur.execute(
            "UPDATE users SET stars = GREATEST(COALESCE(stars, 0) - %s, 0) WHERE user_id = %s",
            (amount, int(user_id)),
        )
    )


def update_withdraw_eligibility_if_needed(user_id: int):
    user = fetch_one("SELECT stars, withdraw_eligible_at FROM users WHERE user_id = %s", (user_id,))
    if not user:
        return
    stars = decimalize(user.get("stars") or 0)
    min_withdraw = min(WITHDRAW_OPTIONS)
    eligible_at = user.get("withdraw_eligible_at")
    if stars >= min_withdraw and not eligible_at:
        execute("UPDATE users SET withdraw_eligible_at = %s WHERE user_id = %s", (now_iso(), user_id))
    elif stars < min_withdraw and eligible_at:
        execute("UPDATE users SET withdraw_eligible_at = NULL WHERE user_id = %s", (user_id,))


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


def get_active_tasks_for_user(user_id: int) -> list[dict]:
    return fetch_all(
        """
        SELECT
            t.id,
            t.channel_title,
            t.link,
            t.reward_stars,
            t.task_type,
            t.chat_username,
            t.post_link,
            t.bot_link,
            t.requires_proof,
            t.proof_required
        FROM tasks t
        LEFT JOIN user_tasks ut
            ON ut.task_id = t.id
           AND ut.user_id = %s
           AND ut.status IN ('completed', 'pending_review')
           AND (ut.status != 'completed' OR ut.reward_removed = 0)
        WHERE t.status = 'active'
          AND ut.id IS NULL
        ORDER BY t.id DESC
        """,
        (int(user_id),),
    )


def get_task_stats_rows() -> list[dict]:
    return fetch_all(
        """
        SELECT
            t.id,
            t.channel_title,
            t.reward_stars,
            t.status,
            t.task_type,
            t.created_at,
            COUNT(CASE WHEN ut.status = 'completed' AND ut.reward_removed = 0 THEN 1 END) AS join_count,
            COUNT(CASE WHEN ut.status = 'pending_review' THEN 1 END) AS pending_count
        FROM tasks t
        LEFT JOIN user_tasks ut ON ut.task_id = t.id
        GROUP BY t.id, t.channel_title, t.reward_stars, t.status, t.task_type, t.created_at
        ORDER BY t.id DESC
        """
    )


def get_active_tasks_admin(limit: int = 200) -> list[dict]:
    return fetch_all(
        "SELECT id, channel_title, reward_stars, status, task_type, created_at FROM tasks WHERE status = 'active' ORDER BY id DESC LIMIT %s",
        (limit,),
    )


def save_referrer_if_new(user_id: int, referrer_id: Optional[int]):
    if not referrer_id or referrer_id == user_id:
        return
    row = get_user(user_id)
    if row and row.get("referrer_id"):
        return
    execute(
        "UPDATE users SET referrer_id = %s WHERE user_id = %s AND (referrer_id IS NULL OR referrer_id = 0)",
        (int(referrer_id), int(user_id)),
    )


def record_referral_bonus_if_needed(user_id: int, task_id: int, reward: Decimal, conn, cur):
    cur.execute("SELECT referrer_id FROM users WHERE user_id = %s", (int(user_id),))
    row = cur.fetchone()
    if not row or not row.get("referrer_id"):
        return
    referrer_id = int(row["referrer_id"])
    if referrer_id == user_id:
        return
    bonus = (decimalize(reward) * REFERRAL_PERCENT / Decimal("100")).quantize(Decimal("0.01"))
    if bonus <= 0:
        return
    cur.execute("UPDATE users SET stars = COALESCE(stars, 0) + %s WHERE user_id = %s", (bonus, referrer_id))


async def is_force_join_ok(bot, user_id: int) -> bool:
    for username, _ in FORCE_JOIN_CHANNELS:
        try:
            member = await bot.get_chat_member(username, user_id)
            if member.status not in ("member", "administrator", "creator"):
                return False
        except Exception:
            return False
    return True


async def channel_member_ok(bot, chat_username: str, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_username, user_id)
        return member.status in ("member", "administrator", "creator")
    except Exception:
        return False


async def bot_is_admin(bot, chat_username: str) -> bool:
    try:
        me = await bot.get_me()
        member = await bot.get_chat_member(chat_username, me.id)
        return member.status in ("administrator", "creator")
    except Exception:
        return False


def task_url(value: str) -> str:
    if value.startswith("@"):
        return f"https://t.me/{value[1:]}"
    return value


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


def mark_task_completed(user_id: int, task_id: int, reward: Decimal) -> None:
    execute(
        """
        INSERT INTO user_tasks
            (user_id, task_id, rewarded_stars, reward_removed, status, created_at, last_checked_at)
        VALUES (%s, %s, %s, 0, 'completed', %s, %s)
        ON CONFLICT (user_id, task_id)
        DO UPDATE SET rewarded_stars = EXCLUDED.rewarded_stars,
                      reward_removed = 0,
                      status = 'completed',
                      last_checked_at = EXCLUDED.last_checked_at,
                      rejection_reason = NULL
        """,
        (user_id, task_id, reward, now_iso(), now_iso()),
    )


def complete_exact_task_reward(user_id: int, task_id: int, reward: Decimal) -> tuple[bool, str]:
    reward = decimalize(reward)

    def _txn(conn, cur):
        cur.execute("SELECT user_id, task_id, rewarded_stars, reward_removed, status FROM user_tasks WHERE user_id = %s AND task_id = %s FOR UPDATE", (user_id, task_id))
        row = cur.fetchone()
        if row and row.get("status") == "completed" and int(row.get("reward_removed") or 0) == 0:
            return False, "already"

        if row:
            cur.execute(
                """
                UPDATE user_tasks
                SET rewarded_stars = %s,
                    reward_removed = 0,
                    status = 'completed',
                    last_checked_at = %s,
                    rejection_reason = NULL
                WHERE user_id = %s AND task_id = %s
                """,
                (reward, now_iso(), user_id, task_id),
            )
        else:
            cur.execute(
                """
                INSERT INTO user_tasks
                    (user_id, task_id, rewarded_stars, reward_removed, status, created_at, last_checked_at)
                VALUES (%s, %s, %s, 0, 'completed', %s, %s)
                """,
                (user_id, task_id, reward, now_iso(), now_iso()),
            )

        cur.execute("UPDATE users SET stars = COALESCE(stars, 0) + %s WHERE user_id = %s", (reward, user_id))
        record_referral_bonus_if_needed(user_id, task_id, reward, conn, cur)
        return True, "ok"

    return transaction(_txn)


def mark_proof_pending(user_id: int, task_id: int, photo_file_id: str, photo_unique_id: str, proof_message_id: int) -> None:
    execute(
        """
        INSERT INTO user_tasks
            (user_id, task_id, rewarded_stars, reward_removed, status, created_at, last_checked_at,
             proof_file_id, proof_file_unique_id, proof_message_id, suspicious)
        VALUES (%s, %s, 0, 1, 'pending_review', %s, %s, %s, %s, %s, 0)
        ON CONFLICT (user_id, task_id)
        DO UPDATE SET status = 'pending_review',
                      reward_removed = 1,
                      proof_file_id = EXCLUDED.proof_file_id,
                      proof_file_unique_id = EXCLUDED.proof_file_unique_id,
                      proof_message_id = EXCLUDED.proof_message_id,
                      last_checked_at = EXCLUDED.last_checked_at,
                      rejection_reason = NULL
        """,
        (user_id, task_id, now_iso(), now_iso(), photo_file_id, photo_unique_id, proof_message_id),
    )


def set_task_removed(task_id: int) -> None:
    execute("UPDATE tasks SET status = 'removed' WHERE id = %s", (task_id,))


def add_task_record(
    task_type: str,
    channel_title: str,
    link: str,
    reward_stars: Decimal,
    chat_username: Optional[str] = None,
    requires_proof: bool = False,
    post_link: Optional[str] = None,
    bot_link: Optional[str] = None,
) -> int:
    row = execute(
        """
        INSERT INTO tasks
            (task_type, channel_title, chat_username, link, reward_stars, status, created_at, requires_proof, proof_required, post_link, bot_link)
        VALUES (%s, %s, %s, %s, %s, 'active', %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (task_type, channel_title, chat_username, link, reward_stars, now_iso(), requires_proof, requires_proof, post_link, bot_link),
        returning=True,
    )
    return int(row["id"])


def withdraw_cooldown_remaining(user_id: int) -> Optional[timedelta]:
    row = fetch_one("SELECT stars, withdraw_eligible_at FROM users WHERE user_id = %s", (user_id,))
    if not row:
        return None
    stars = decimalize(row.get("stars") or 0)
    eligible_at = parse_dt(row.get("withdraw_eligible_at"))
    min_withdraw = min(WITHDRAW_OPTIONS)
    if stars < min_withdraw or not eligible_at:
        return None
    unlocked_at = eligible_at + timedelta(hours=WITHDRAW_COOLDOWN_HOURS)
    if now_utc() >= unlocked_at:
        return timedelta(seconds=0)
    return unlocked_at - now_utc()


# =====================================
# UI
# =====================================
def main_menu(user_id: int) -> ReplyKeyboardMarkup:
    keyboard = [
        ["🏧 Withdraw", "⭐ My Stars"],
        ["👥 Referral", "📝 Tasks"],
        ["🎁 Bonus", "🌐 Language"],
        ["ℹ️ About Us", "📞 Support"],
    ]
    if user_id == ADMIN_ID:
        keyboard.insert(0, ["📊 Statistics", "📣 Broadcast"])
        keyboard.insert(1, ["🛠 Add Task", "🗑 Remove Task"])
        keyboard.insert(2, ["➕ Add Balance"])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def cancel_reply_keyboard():
    return ReplyKeyboardMarkup([["⬅️ Back"]], resize_keyboard=True)


def add_task_kind_keyboard(user_id: int):
    lang = get_lang(user_id)
    rows = [
        [
            InlineKeyboardButton("Channel/Group", callback_data="task_channel"),
            InlineKeyboardButton("Reaction", callback_data="task_reaction"),
        ],
        [
            InlineKeyboardButton("Bot Link", callback_data="task_bot_link"),
            InlineKeyboardButton("YouTube", callback_data="task_youtube"),
        ],
        [InlineKeyboardButton("Facebook", callback_data="task_facebook")],
    ]
    return InlineKeyboardMarkup(rows)


def force_join_keyboard(user_id: int):
    rows = [[InlineKeyboardButton(username, url=url)] for username, url in FORCE_JOIN_CHANNELS]
    rows.append([InlineKeyboardButton(t(user_id, "joined_btn"), callback_data="joined_ok")])
    return InlineKeyboardMarkup(rows)


def task_buttons(user_id: int, task: dict):
    task_id = int(task["id"])
    open_link = task.get("post_link") or task.get("bot_link") or task.get("link")
    rows = [[InlineKeyboardButton("🔗 Open", url=open_link)]]
    task_type = task.get("task_type")
    if task_type in PROOF_TASK_TYPES or task.get("requires_proof"):
        rows.append([InlineKeyboardButton("📤 Send Proof", callback_data=f"task_proof_{task_id}")])
    else:
        rows.append([InlineKeyboardButton("✅ Verify", callback_data=f"verify_{task_id}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="tasks_back")])
    return InlineKeyboardMarkup(rows)


# =====================================
# TEXTS
# =====================================
TEXTS = {
    "ps": {
        "choose_lang": "ژبه انتخاب کړئ:",
        "intro": "ښه راغلاست EasyEarn Bot ته",
        "force_join": "مهرباني وکړئ ټول چینلونه جوین کړئ:",
        "joined_btn": "✅ جوین مې کړل",
        "join_failed": "اول ټول اړین چینلونه جوین کړئ.",
        "my_stars": "⭐ ستاسو ستوري: {stars}",
        "referral": "👥 ستاسو ریفرل لینک:\n{link}\n\nتاسو به د خپلو ریفرلونو له عاید څخه 15% ترلاسه کوئ.\nجعلي ریفرل نه منل کېږي، که وپېژندل شي ستاسو اکاونټ به بند شي.\n\nټول ریفرلونه: {count}",
        "tasks_empty": "❌ فعلاً هېڅ تاسک نشته",
        "task_done": "✅ تاسک بشپړ شو\n⭐ {stars}",
        "task_already": "تاسو دا تاسک مخکې بشپړ کړی",
        "task_fail": "❌ لومړی چینل جوین کړه، بیا تایید وکړه",
        "bonus_added": "✅ ورځنی بونس واخیستل شو: {stars} ⭐",
        "bonus_wait": "⏳ بونس مخکې اخیستل شوی.\nپاتې وخت: {time}",
        "withdraw_choose": "د ویډرا اندازه انتخاب کړئ:",
        "withdraw_low": "ستوري کافي نه دي.",
        "withdraw_sent": "✅ ستاسو ویډرا د اډمین تایید ته ولېږل شوه.",
        "withdraw_wait": "⏳ ویډرا اوس نشئ کولی.\nپاتې انتظار: {time}",
        "about": "ℹ️ EasyEarn Bot\nد چینل/ګروپ او نورو تاسکونو له لارې ستوري ترلاسه کړئ.",
        "support": f"📞 Support: {SUPPORT_USERNAME}",
        "lang_saved": "✅ ژبه بدله شوه",
        "new_task_notify": "🆕 نوی تاسک اضافه شو. مهرباني وکړئ بشپړ یې کړئ.",
        "proof_send": "مهرباني وکړئ د دې تاسک سکرین شاټ دلته راولېږئ.",
        "proof_pending": "⏳ ستاسو ثبوت اډمین ته ولېږل شو. د تایید انتظار وکړئ.",
        "proof_rejected": "❌ ستاسو ثبوت رد شو. تاسک بیا موجود دی.",
        "proof_approved": "✅ ستاسو ثبوت تایید شو. ⭐ {stars}",
        "banned": "⛔ تاسو له دې بوټ څخه بند شوي یاست.",
        "addtask_kind": "د task ډول انتخاب کړئ:",
        "addtask_link": "د چینل/ګروپ لینک یا @username راولېږئ.",
        "addtask_title": "د task عنوان راولېږئ.",
        "addtask_reward": "ریوارډ ولیکئ، مثال: 0.5",
        "addtask_post_link": "د post لینک راولېږئ.",
        "addtask_bot_link": "د bot لینک راولېږئ. مثال: https://t.me/SomeBot?start=abc",
    },
    "en": {
        "choose_lang": "Choose language:",
        "intro": "Welcome to EasyEarn Bot",
        "force_join": "Please join all required channels:",
        "joined_btn": "✅ I Joined",
        "join_failed": "Please join all required channels first.",
        "my_stars": "⭐ Your stars: {stars}",
        "referral": "👥 Your referral link:\n{link}\n\nYou earn 15% from valid referral earnings.\nFake referrals are not accepted and may lead to ban.\n\nTotal referrals: {count}",
        "tasks_empty": "❌ No tasks available right now",
        "task_done": "✅ Task completed\n⭐ {stars}",
        "task_already": "You already completed this task",
        "task_fail": "❌ Join the channel first, then verify",
        "bonus_added": "✅ Daily bonus added: {stars} ⭐",
        "bonus_wait": "⏳ Bonus already claimed.\nRemaining: {time}",
        "withdraw_choose": "Choose withdraw amount:",
        "withdraw_low": "Not enough stars.",
        "withdraw_sent": "✅ Your withdraw request has been sent for admin review.",
        "withdraw_wait": "⏳ Withdraw is locked right now.\nRemaining wait: {time}",
        "about": "ℹ️ EasyEarn Bot\nEarn stars through channel/group and other tasks.",
        "support": f"📞 Support: {SUPPORT_USERNAME}",
        "lang_saved": "✅ Language updated",
        "new_task_notify": "🆕 A new task has been added. Please complete it.",
        "proof_send": "Please send your screenshot proof here.",
        "proof_pending": "⏳ Your proof was sent to admin. Please wait for review.",
        "proof_rejected": "❌ Your proof was rejected. The task is available again.",
        "proof_approved": "✅ Your proof was approved. ⭐ {stars}",
        "banned": "⛔ You are banned from using this bot.",
        "addtask_kind": "Choose task type:",
        "addtask_link": "Send channel/group link or @username.",
        "addtask_title": "Send task title.",
        "addtask_reward": "Send reward, example: 0.5",
        "addtask_post_link": "Send the post link.",
        "addtask_bot_link": "Send the bot link. Example: https://t.me/SomeBot?start=abc",
    },
}


def t(user_id: int, key: str, **kwargs) -> str:
    lang = get_lang(user_id)
    base = TEXTS.get(lang, TEXTS["ps"]).get(key) or TEXTS["ps"].get(key, key)
    return base.format(**kwargs)


# =====================================
# COMMANDS
# =====================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    user = update.effective_user
    ensure_user(user.id, user.username, user.full_name)
    if is_banned(user.id):
        await update.message.reply_text(t(user.id, "banned"))
        return

    if context.args:
        arg = context.args[0]
        m = re.match(r"ref_(\d+)", arg)
        if m:
            save_referrer_if_new(user.id, int(m.group(1)))

    if not await is_force_join_ok(context.bot, user.id):
        await update.message.reply_text(t(user.id, "force_join"), reply_markup=force_join_keyboard(user.id))
        return

    await update.message.reply_text(t(user.id, "intro"), reply_markup=main_menu(user.id))


async def users_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    row = fetch_one("SELECT COUNT(*) AS c FROM users", ())
    await update.message.reply_text(f"Users: {int(row['c'])}")


async def refstats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    rows = top_referrals(50)
    if not rows:
        await update.message.reply_text("No referral data.")
        return
    lines = ["Referral Stats:"]
    for r in rows:
        inviter = get_user(int(r["referrer_id"]))
        refs = get_user_refs(int(r["referrer_id"]))
        invited = ", ".join([f"{u.get('username') or u['id']}" for u in refs]) or "-"
        lines.append(
            f"User {r['referrer_id']} ({(inviter or {}).get('username') or 'no-username'}) -> {int(r['refs'])} invited\nInvited: {invited}"
        )
    await update.message.reply_text("\n\n".join(lines[:20]))


async def withdraws_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    rows = fetch_all("SELECT * FROM withdrawals ORDER BY id DESC LIMIT 30")
    if not rows:
        await update.message.reply_text("No withdrawals.")
        return
    lines = []
    for w in rows:
        lines.append(f"#{w['id']} | user {w['user_id']} | {pretty_amount(w['amount_stars'])} | {w['status']} | {now_pretty(w['created_at'])}")
    await update.message.reply_text("\n".join(lines))


async def botstats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    users = fetch_one("SELECT COUNT(*) AS c FROM users", ())
    tasks = fetch_one("SELECT COUNT(*) AS c FROM tasks WHERE status = 'active'", ())
    pending = fetch_one("SELECT COUNT(*) AS c FROM withdrawals WHERE status = 'pending'", ())
    await update.message.reply_text(
        f"Users: {int(users['c'])}\nActive tasks: {int(tasks['c'])}\nPending withdraws: {int(pending['c'])}"
    )


async def taskslist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    rows = get_task_stats_rows()
    if not rows:
        await update.message.reply_text("No tasks.")
        return
    lines = []
    for row in rows[:50]:
        duration = human_remaining(timedelta(hours=max(0, hours_since(row.get("created_at"))))).replace("h", "h active, ")
        lines.append(
            f"#{row['id']} | {row['channel_title']} | {row['task_type']} | ⭐ {pretty_amount(row['reward_stars'])} | joins: {int(row['join_count'])} | active: {duration}"
        )
    await update.message.reply_text("\n".join(lines))


async def taskstats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    rows = get_task_stats_rows()
    if not rows:
        await update.message.reply_text("No task stats.")
        return
    text = []
    for row in rows[:30]:
        duration = hours_since(row["created_at"])
        text.append(
            f"#{row['id']}\n"
            f"title: {row['channel_title']}\n"
            f"🧩 type: {row['task_type']}\n"
            f"⭐ reward: {pretty_amount(row['reward_stars'])}\n"
            f"✅ completed: {int(row['join_count'])}\n"
            f"⏳ pending: {int(row['pending_count'])}\n"
            f"🕒 active hours: {int(duration)}"
        )
    await update.message.reply_text("\n\n".join(text))


# =====================================
# TASK / LEAVE CHECKS
# =====================================
async def process_leave_penalties(context: ContextTypes.DEFAULT_TYPE):
    rows = fetch_all(
        """
        SELECT ut.id, ut.user_id, ut.task_id, ut.rewarded_stars, t.chat_username, t.task_type
        FROM user_tasks ut
        JOIN tasks t ON t.id = ut.task_id
        WHERE ut.status = 'completed'
          AND ut.reward_removed = 0
          AND t.task_type = 'channel'
          AND t.status = 'active'
        """
    )

    for row in rows:
        user_id = int(row["user_id"])
        task_id = int(row["task_id"])
        chat_username = row.get("chat_username")
        reward = decimalize(row.get("rewarded_stars") or 0)
        if not chat_username:
            continue

        still_joined = await channel_member_ok(context.bot, chat_username, user_id)
        if still_joined:
            continue

        def _txn(conn, cur):
            cur.execute("SELECT reward_removed FROM user_tasks WHERE id = %s FOR UPDATE", (int(row["id"]),))
            state = cur.fetchone()
            if not state or int(state.get("reward_removed") or 0) == 1:
                return False
            cur.execute(
                "UPDATE user_tasks SET reward_removed = 1, last_checked_at = %s WHERE id = %s",
                (now_iso(), int(row["id"])),
            )
            cur.execute(
                "UPDATE users SET stars = GREATEST(COALESCE(stars, 0) - %s, 0) WHERE user_id = %s",
                (reward, user_id),
            )
            return True

        changed = transaction(_txn)
        if changed:
            update_withdraw_eligibility_if_needed(user_id)
            try:
                await context.bot.send_message(
                    user_id,
                    "⚠️ You left a rewarded channel. Your reward was deducted. Please join again to restore it.",
                    reply_markup=main_menu(user_id),
                )
            except Exception:
                pass


async def promo_job(context: ContextTypes.DEFAULT_TYPE):
    rows = fetch_all("SELECT chat_id FROM promo_chats WHERE is_active = 1")
    for row in rows:
        try:
            await context.bot.send_message(int(row["chat_id"]), PROMO_TEXT)
        except Exception:
            deactivate_promo_chat(int(row["chat_id"]))


async def leave_check_job(context: ContextTypes.DEFAULT_TYPE):
    await process_leave_penalties(context)


# =====================================
# TASK DISPLAY
# =====================================
def task_title_line(task: dict) -> str:
    reward = pretty_amount(task["reward_stars"])
    task_type = task.get("task_type") or "channel"
    if get_lang(ADMIN_ID) == "ps":
        kind = {
            "channel": "چینل/ګروپ",
            "reaction": "ریاکشن",
            "bot_link": "بوټ لینک",
            "youtube": "یوټیوب",
            "facebook": "فېسبوک",
        }.get(task_type, task_type)
    else:
        kind = {
            "channel": "Channel/Group",
            "reaction": "Reaction",
            "bot_link": "Bot Link",
            "youtube": "YouTube",
            "facebook": "Facebook",
        }.get(task_type, task_type)
    return f"#{task['id']} | {task['channel_title']} | {kind} | ⭐ {reward}"


async def send_tasks_page(message, user_id: int):
    tasks = get_active_tasks_for_user(user_id)
    if not tasks:
        await message.reply_text(t(user_id, "tasks_empty"), reply_markup=main_menu(user_id))
        return

    for task in tasks:
        text = task_title_line(task)
        await message.reply_text(text, reply_markup=task_buttons(user_id, task))


# =====================================
# CALLBACKS
# =====================================
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    ensure_user(user.id, user.username, user.full_name)

    if is_banned(user.id):
        await query.message.reply_text(t(user.id, "banned"))
        return

    data = query.data or ""

    if data == "joined_ok":
        if not await is_force_join_ok(context.bot, user.id):
            await query.message.reply_text(t(user.id, "join_failed"), reply_markup=force_join_keyboard(user.id))
            return
        await query.message.reply_text(t(user.id, "intro"), reply_markup=main_menu(user.id))
        return

    if data == "tasks_back":
        try:
            await query.message.delete()
        except Exception:
            pass
        return

    if data.startswith("verify_"):
        task_id = int(data.split("_")[1])
        task = get_task(task_id)
        if not task or task.get("status") != "active":
            await query.message.reply_text("Task not found.")
            return

        if task.get("task_type") != "channel":
            await query.message.reply_text("This task requires proof review.")
            return

        chat_username = task.get("chat_username")
        if not chat_username:
            await query.message.reply_text("Task config is invalid.")
            return

        if not await bot_is_admin(context.bot, chat_username):
            await query.message.reply_text("Bot is not admin in that channel/group.")
            return

        joined = await channel_member_ok(context.bot, chat_username, user.id)
        if not joined:
            await query.message.reply_text(t(user.id, "task_fail"))
            return

        ok, status = complete_exact_task_reward(user.id, int(task_id), decimalize(task["reward_stars"]))
        update_withdraw_eligibility_if_needed(user.id)

        if not ok and status == "already":
            await query.message.reply_text(t(user.id, "task_already"), reply_markup=main_menu(user.id))
            try:
                await query.message.delete()
            except Exception:
                pass
            return

        await query.message.reply_text(
            t(user.id, "task_done", stars=pretty_amount(task["reward_stars"])),
            reply_markup=main_menu(user.id),
        )
        try:
            await query.message.delete()
        except Exception:
            pass
        return

    if data.startswith("task_proof_"):
        task_id = int(data.split("_")[2])
        task = get_task(task_id)
        if not task or task.get("status") != "active":
            await query.message.reply_text("Task not found.")
            return
        context.user_data["awaiting_proof_task_id"] = task_id
        await query.message.reply_text(t(user.id, "proof_send"), reply_markup=cancel_reply_keyboard())
        return

    if data.startswith("proof_approve_"):
        if user.id != ADMIN_ID:
            return
        user_task_id = int(data.split("_")[2])
        row = fetch_one(
            """
            SELECT ut.*, t.reward_stars, t.channel_title, t.task_type, t.chat_username
            FROM user_tasks ut
            JOIN tasks t ON t.id = ut.task_id
            WHERE ut.id = %s
            """,
            (user_task_id,),
        )
        if not row or row.get("status") != "pending_review":
            await query.message.reply_text("Proof not found.")
            return

        task_type = row.get("task_type") or "channel"
        if task_type == "channel":
            chat_username = row.get("chat_username")
            if not chat_username or not await bot_is_admin(context.bot, chat_username) or not await channel_member_ok(context.bot, chat_username, int(row["user_id"])):
                await query.message.reply_text("User is not verified in channel/group.")
                return

        ok, reason = complete_exact_task_reward(int(row["user_id"]), int(row["task_id"]), decimalize(row["reward_stars"]))
        execute(
            "UPDATE user_tasks SET status = 'completed', reviewed_at = %s, reviewed_by = %s, reward_removed = 0 WHERE id = %s",
            (now_iso(), ADMIN_ID, user_task_id),
        )
        update_withdraw_eligibility_if_needed(int(row["user_id"]))

        try:
            await query.edit_message_caption((query.message.caption or "") + "\n\n✅ APPROVED")
        except Exception:
            try:
                await query.edit_message_text((query.message.text or "") + "\n\n✅ APPROVED")
            except Exception:
                pass

        try:
            await context.bot.send_message(int(row["user_id"]), t(int(row["user_id"]), "proof_approved", stars=pretty_amount(row["reward_stars"])), reply_markup=main_menu(int(row["user_id"])))
        except Exception:
            pass
        return

    if data.startswith("proof_reject_"):
        if user.id != ADMIN_ID:
            return
        user_task_id = int(data.split("_")[2])
        row = fetch_one(
            """
            SELECT ut.*, t.reward_stars, t.channel_title
            FROM user_tasks ut
            JOIN tasks t ON t.id = ut.task_id
            WHERE ut.id = %s
            """,
            (user_task_id,),
        )
        if not row:
            await query.message.reply_text("Proof not found.")
            return

        execute(
            """
            UPDATE user_tasks
            SET status = 'rejected',
                reviewed_at = %s,
                reviewed_by = %s,
                reward_removed = 1
            WHERE id = %s
            """,
            (now_iso(), ADMIN_ID, user_task_id),
        )

        try:
            await query.edit_message_caption((query.message.caption or "") + "\n\n❌ REJECTED")
        except Exception:
            try:
                await query.edit_message_text((query.message.text or "") + "\n\n❌ REJECTED")
            except Exception:
                pass

        try:
            await context.bot.send_message(int(row["user_id"]), t(int(row["user_id"]), "proof_rejected"), reply_markup=main_menu(int(row["user_id"])))
        except Exception:
            pass
        return

    if data == "task_channel":
        context.user_data["new_task_type"] = "channel"
        context.user_data["task_type"] = "channel"
        context.user_data["admin_flow"] = "addtask_link"
        await query.message.reply_text(t(user.id, "addtask_link"), reply_markup=cancel_reply_keyboard())
        return

    if data == "task_reaction":
        context.user_data["new_task_type"] = "reaction"
        context.user_data["task_type"] = "reaction"
        context.user_data["admin_flow"] = "addtask_post_link"
        await query.message.reply_text(t(user.id, "addtask_post_link"), reply_markup=cancel_reply_keyboard())
        return

    if data == "task_bot_link":
        context.user_data["new_task_type"] = "bot_link"
        context.user_data["task_type"] = "bot_link"
        context.user_data["admin_flow"] = "addtask_bot_link"
        await query.message.reply_text(t(user.id, "addtask_bot_link"), reply_markup=cancel_reply_keyboard())
        return

    if data == "task_youtube":
        context.user_data["new_task_type"] = "youtube"
        context.user_data["task_type"] = "youtube"
        context.user_data["admin_flow"] = "addtask_link"
        await query.message.reply_text(t(user.id, "addtask_link"), reply_markup=cancel_reply_keyboard())
        return

    if data == "task_facebook":
        context.user_data["new_task_type"] = "facebook"
        context.user_data["task_type"] = "facebook"
        context.user_data["admin_flow"] = "addtask_link"
        await query.message.reply_text(t(user.id, "addtask_link"), reply_markup=cancel_reply_keyboard())
        return

    if data.startswith("remove_task_"):
        if user.id != ADMIN_ID:
            return
        task_id = int(data.split("_")[2])
        set_task_removed(task_id)
        await query.message.reply_text(f"✅ Removed task #{task_id}", reply_markup=main_menu(user.id))
        return

    if data.startswith("wd_approve_"):
        if user.id != ADMIN_ID:
            return
        wd_id = int(data.split("_")[2])
        wd = fetch_one("SELECT * FROM withdrawals WHERE id = %s", (wd_id,))
        if not wd or wd.get("status") != "pending":
            await query.message.reply_text("Withdraw not found.")
            return
        execute(
            "UPDATE withdrawals SET status = 'approved', approved_at = %s WHERE id = %s",
            (now_iso(), wd_id),
        )
        try:
            await query.edit_message_text((query.message.text or "") + "\n\n✅ APPROVED")
        except Exception:
            pass
        if wd.get("channel_message_id"):
            try:
                await context.bot.edit_message_text(
                    chat_id=PAYMENT_CHANNEL,
                    message_id=int(wd["channel_message_id"]),
                    text=f"✅ Approved\nUser: {wd['user_id']}\nAmount: {pretty_amount(wd['amount_stars'])}",
                )
            except Exception:
                pass
        try:
            await context.bot.send_message(int(wd["user_id"]), "✅ Withdraw approved.", reply_markup=main_menu(int(wd["user_id"])))
        except Exception:
            pass
        return

    if data.startswith("wd_reject_"):
        if user.id != ADMIN_ID:
            return
        wd_id = int(data.split("_")[2])
        wd = fetch_one("SELECT * FROM withdrawals WHERE id = %s", (wd_id,))
        if not wd or wd.get("status") != "pending":
            await query.message.reply_text("Withdraw not found.")
            return

        def _txn(conn, cur):
            cur.execute("UPDATE withdrawals SET status = 'rejected', rejected_at = %s WHERE id = %s", (now_iso(), wd_id))
            cur.execute("UPDATE users SET stars = COALESCE(stars, 0) + %s WHERE user_id = %s", (decimalize(wd["amount_stars"]), int(wd["user_id"])))

        transaction(_txn)
        update_withdraw_eligibility_if_needed(int(wd["user_id"]))
        try:
            await query.edit_message_text((query.message.text or "") + "\n\n❌ REJECTED")
        except Exception:
            pass
        if wd.get("channel_message_id"):
            try:
                await context.bot.edit_message_text(
                    chat_id=PAYMENT_CHANNEL,
                    message_id=int(wd["channel_message_id"]),
                    text=f"❌ Rejected\nUser: {wd['user_id']}\nAmount: {pretty_amount(wd['amount_stars'])}",
                )
            except Exception:
                pass
        try:
            await context.bot.send_message(int(wd["user_id"]), "❌ Withdraw rejected. Amount returned.", reply_markup=main_menu(int(wd["user_id"])))
        except Exception:
            pass
        return


# =====================================
# MESSAGE HANDLER
# =====================================
async def handle_photo_proof(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.username, user.full_name)

    if is_banned(user.id):
        await update.message.reply_text(t(user.id, "banned"))
        return

    task_id = context.user_data.get("awaiting_proof_task_id")
    if not task_id:
        return

    task = get_task(int(task_id))
    if not task or task.get("status") != "active":
        context.user_data.pop("awaiting_proof_task_id", None)
        await update.message.reply_text("Task not found.", reply_markup=main_menu(user.id))
        return

    photo = update.message.photo[-1]
    caption = (
        f"📥 Proof Review\n"
        f"User: {user.id} (@{user.username or 'no_username'})\n"
        f"Task: #{task['id']} | {task['channel_title']}\n"
        f"Type: {task.get('task_type')}\n"
        f"Reward: {pretty_amount(task['reward_stars'])}"
    )
    sent = await context.bot.send_photo(
        ADMIN_ID,
        photo=photo.file_id,
        caption=caption,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("✅ Approve", callback_data=f"proof_approve_{0}"),
                    InlineKeyboardButton("❌ Reject", callback_data=f"proof_reject_{0}"),
                ]
            ]
        ),
    )

    mark_proof_pending(user.id, int(task_id), photo.file_id, photo.file_unique_id, update.message.message_id)
    row = fetch_one(
        "SELECT id FROM user_tasks WHERE user_id = %s AND task_id = %s",
        (user.id, int(task_id)),
    )
    if row:
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("✅ Approve", callback_data=f"proof_approve_{row['id']}"),
                    InlineKeyboardButton("❌ Reject", callback_data=f"proof_reject_{row['id']}"),
                ]
            ]
        )
        try:
            await context.bot.edit_message_reply_markup(ADMIN_ID, sent.message_id, reply_markup=kb)
        except Exception:
            pass

    context.user_data.pop("awaiting_proof_task_id", None)
    await update.message.reply_text(t(user.id, "proof_pending"), reply_markup=main_menu(user.id))


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return

    user = update.effective_user
    text = (update.message.text or "").strip()

    ensure_user(user.id, user.username, user.full_name)
    if is_banned(user.id):
        await update.message.reply_text(t(user.id, "banned"))
        return

    if not await is_force_join_ok(context.bot, user.id):
        await update.message.reply_text(t(user.id, "force_join"), reply_markup=force_join_keyboard(user.id))
        return

    if text == "⬅️ Back":
        for key in [
            "admin_flow",
            "new_task_type",
            "task_type",
            "task_chat_username",
            "task_link",
            "task_title",
            "task_post_link",
            "task_bot_link",
            "awaiting_proof_task_id",
            "add_balance_user_id",
            "withdraw_user_id",
            "broadcast_waiting",
        ]:
            context.user_data.pop(key, None)
        await update.message.reply_text(t(user.id, "intro"), reply_markup=main_menu(user.id))
        return

    if text in ("⭐ My Stars", "⭐ زما ستوري", "⭐ ستوري"):
        await update.message.reply_text(t(user.id, "my_stars", stars=pretty_amount(get_stars(user.id))), reply_markup=main_menu(user.id))
        return

    if text in ("👥 Referral", "👥 ریفرل"):
        await update.message.reply_text(
            t(user.id, "referral", link=referral_link(user.id), count=referral_count(user.id)),
            reply_markup=main_menu(user.id),
        )
        return

    if text in ("📝 Tasks", "📝 تاسکونه"):
        await send_tasks_page(update.message, user.id)
        return

    if text in ("🎁 Bonus", "🎁 بونس"):
        row = get_user(user.id)
        last_bonus_at = parse_dt(row.get("last_bonus_at") if row else None)
        if last_bonus_at and now_utc() < last_bonus_at + timedelta(hours=BONUS_INTERVAL_HOURS):
            remain = (last_bonus_at + timedelta(hours=BONUS_INTERVAL_HOURS)) - now_utc()
            await update.message.reply_text(t(user.id, "bonus_wait", time=human_remaining(remain)), reply_markup=main_menu(user.id))
            return
        execute("UPDATE users SET stars = COALESCE(stars, 0) + %s, last_bonus_at = %s WHERE user_id = %s", (DAILY_BONUS_STARS, now_iso(), user.id))
        update_withdraw_eligibility_if_needed(user.id)
        await update.message.reply_text(t(user.id, "bonus_added", stars=pretty_amount(DAILY_BONUS_STARS)), reply_markup=main_menu(user.id))
        return

    if text in ("🏧 Withdraw", "🏧 ویډرا"):
        stars = get_stars(user.id)
        if stars < min(WITHDRAW_OPTIONS):
            await update.message.reply_text(t(user.id, "withdraw_low"), reply_markup=main_menu(user.id))
            return
        remain = withdraw_cooldown_remaining(user.id)
        if remain and remain.total_seconds() > 0:
            await update.message.reply_text(t(user.id, "withdraw_wait", time=human_remaining(remain)), reply_markup=main_menu(user.id))
            return
        rows = [[InlineKeyboardButton(pretty_amount(a), callback_data=f"withdraw_{pretty_amount(a)}")] for a in WITHDRAW_OPTIONS]
        rows.append([InlineKeyboardButton("⬅️ Back", callback_data="tasks_back")])
        await update.message.reply_text(t(user.id, "withdraw_choose"), reply_markup=InlineKeyboardMarkup(rows))
        return

    if text in ("🌐 Language", "🌐 ژبه"):
        kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("پښتو", callback_data="lang_ps"), InlineKeyboardButton("English", callback_data="lang_en")]
            ]
        )
        await update.message.reply_text(t(user.id, "choose_lang"), reply_markup=kb)
        return

    if text in ("ℹ️ About Us", "ℹ️ زموږ په اړه"):
        await update.message.reply_text(t(user.id, "about"), reply_markup=main_menu(user.id))
        return

    if text in ("📞 Support", "📞 ملاتړ"):
        await update.message.reply_text(t(user.id, "support"), reply_markup=main_menu(user.id))
        return

    if text == "📊 Statistics" and user.id == ADMIN_ID:
        await botstats_cmd(update, context)
        return

    if text == "📣 Broadcast" and user.id == ADMIN_ID:
        context.user_data["broadcast_waiting"] = True
        await update.message.reply_text("Send broadcast text now.", reply_markup=cancel_reply_keyboard())
        return

    if text == "🛠 Add Task" and user.id == ADMIN_ID:
        context.user_data.clear()
        await update.message.reply_text(t(user.id, "addtask_kind"), reply_markup=add_task_kind_keyboard(user.id))
        return

    if text == "🗑 Remove Task" and user.id == ADMIN_ID:
        tasks = fetch_all("SELECT id, channel_title, task_type FROM tasks WHERE status = 'active' ORDER BY id DESC")
        if not tasks:
            await update.message.reply_text("No active tasks.", reply_markup=main_menu(user.id))
            return
        buttons_list = [[InlineKeyboardButton(f"#{task['id']} - {task['channel_title']} ({task['task_type']})", callback_data=f"remove_task_{task['id']}")] for task in tasks]
        await update.message.reply_text("Choose task to remove:", reply_markup=InlineKeyboardMarkup(buttons_list))
        return

    if text == "➕ Add Balance" and user.id == ADMIN_ID:
        context.user_data["admin_flow"] = "add_balance_user"
        await update.message.reply_text("Send user ID.", reply_markup=cancel_reply_keyboard())
        return

    if context.user_data.get("broadcast_waiting") and user.id == ADMIN_ID:
        rows = fetch_all("SELECT user_id FROM users WHERE is_banned = 0")
        sent = 0
        failed = 0
        for r in rows:
            try:
                await context.bot.send_message(int(r["user_id"]), text)
                sent += 1
            except Exception:
                failed += 1
        context.user_data.pop("broadcast_waiting", None)
        await update.message.reply_text(f"✅ Sent: {sent}\n❌ Failed: {failed}", reply_markup=main_menu(update.effective_user.id))
        return

    flow = context.user_data.get("admin_flow")

    if flow == "add_balance_user" and user.id == ADMIN_ID:
        if not text.isdigit():
            await update.message.reply_text("Send a valid user ID.", reply_markup=cancel_reply_keyboard())
            return
        context.user_data["add_balance_user_id"] = int(text)
        context.user_data["admin_flow"] = "add_balance_amount"
        await update.message.reply_text("Send amount.", reply_markup=cancel_reply_keyboard())
        return

    if flow == "add_balance_amount" and user.id == ADMIN_ID:
        try:
            amount = decimalize(text)
            if amount <= 0:
                raise ValueError
        except Exception:
            await update.message.reply_text("Invalid amount.", reply_markup=cancel_reply_keyboard())
            return
        target_id = int(context.user_data["add_balance_user_id"])
        add_stars(target_id, amount)
        context.user_data.pop("admin_flow", None)
        context.user_data.pop("add_balance_user_id", None)
        await update.message.reply_text("✅ Balance added.", reply_markup=main_menu(user.id))
        return

    if flow == "addtask_link":
        task_type = context.user_data.get("task_type") or context.user_data.get("new_task_type") or "channel"
        if task_type == "channel":
            username = extract_chat_username(text)
            if not username:
                await update.message.reply_text("Invalid link or @username", reply_markup=cancel_reply_keyboard())
                return
            context.user_data["task_chat_username"] = username
            context.user_data["task_link"] = task_url(text)
        else:
            if not text.startswith("http"):
                await update.message.reply_text("Send a valid public link.", reply_markup=cancel_reply_keyboard())
                return
            context.user_data["task_chat_username"] = None
            context.user_data["task_link"] = text
        context.user_data["admin_flow"] = "addtask_title"
        await update.message.reply_text(t(update.effective_user.id, "addtask_title"), reply_markup=cancel_reply_keyboard())
        return

    if flow == "addtask_post_link":
        if not text.startswith("http"):
            await update.message.reply_text("Send a valid public post link.", reply_markup=cancel_reply_keyboard())
            return
        context.user_data["task_post_link"] = text
        context.user_data["task_link"] = text
        context.user_data["admin_flow"] = "addtask_title"
        await update.message.reply_text(t(update.effective_user.id, "addtask_title"), reply_markup=cancel_reply_keyboard())
        return

    if flow == "addtask_bot_link":
        if not text.startswith("http"):
            await update.message.reply_text("Send a valid bot link.", reply_markup=cancel_reply_keyboard())
            return
        context.user_data["task_bot_link"] = text
        context.user_data["task_link"] = text
        context.user_data["admin_flow"] = "addtask_title"
        await update.message.reply_text(t(update.effective_user.id, "addtask_title"), reply_markup=cancel_reply_keyboard())
        return

    if flow == "addtask_title":
        context.user_data["task_title"] = text
        context.user_data["admin_flow"] = "addtask_reward"
        await update.message.reply_text(t(update.effective_user.id, "addtask_reward"), reply_markup=cancel_reply_keyboard())
        return

    if flow == "addtask_reward":
        try:
            reward = decimalize(text)
            if reward <= 0:
                raise ValueError
        except Exception:
            await update.message.reply_text("Invalid reward. Example: 0.5", reply_markup=cancel_reply_keyboard())
            return

        task_type = (context.user_data.get("task_type") or context.user_data.get("new_task_type") or "channel").strip().lower()
        if task_type == "group":
            task_type = "channel"

        title = (context.user_data.get("task_title") or "Task").strip()
        task_link = context.user_data.get("task_link")
        chat_username = context.user_data.get("task_chat_username")
        post_link = context.user_data.get("task_post_link")
        bot_link = context.user_data.get("task_bot_link")
        requires_proof = task_type in {"reaction", "facebook", "youtube"}

        created_id = add_task_record(
            task_type=task_type,
            channel_title=title,
            link=task_link,
            reward_stars=reward,
            chat_username=chat_username,
            requires_proof=requires_proof,
            post_link=post_link,
            bot_link=bot_link,
        )

        for key in [
            "admin_flow",
            "new_task_type",
            "task_type",
            "task_chat_username",
            "task_link",
            "task_title",
            "task_post_link",
            "task_bot_link",
        ]:
            context.user_data.pop(key, None)

        rows = fetch_all("SELECT user_id FROM users WHERE is_banned = 0")
        for r in rows:
            try:
                await context.bot.send_message(int(r["user_id"]), t(int(r["user_id"]), "new_task_notify"))
            except Exception:
                pass

        await update.message.reply_text(f"✅ Task #{created_id} added.", reply_markup=main_menu(user.id))
        return

    if text.startswith("/ban ") and user.id == ADMIN_ID:
        parts = text.split(maxsplit=2)
        if len(parts) >= 2 and parts[1].isdigit():
            target = int(parts[1])
            reason = parts[2] if len(parts) > 2 else ""
            ban_user(target, reason)
            await update.message.reply_text(f"✅ Banned {target}", reply_markup=main_menu(user.id))
        return

    if text.startswith("/unban ") and user.id == ADMIN_ID:
        parts = text.split(maxsplit=1)
        if len(parts) == 2 and parts[1].isdigit():
            target = int(parts[1])
            unban_user(target)
            await update.message.reply_text(f"✅ Unbanned {target}", reply_markup=main_menu(user.id))
        return


async def lang_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    if data not in ("lang_ps", "lang_en"):
        return
    lang = data.split("_")[1]
    set_lang(update.effective_user.id, lang)
    await query.answer("OK")
    await query.message.reply_text(t(update.effective_user.id, "lang_saved"), reply_markup=main_menu(update.effective_user.id))


async def withdraw_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    data = query.data or ""
    if not data.startswith("withdraw_"):
        return
    amount = decimalize(data.split("_", 1)[1])
    stars = get_stars(user.id)
    if stars < amount:
        await query.message.reply_text(t(user.id, "withdraw_low"))
        return
    remain = withdraw_cooldown_remaining(user.id)
    if remain and remain.total_seconds() > 0:
        await query.message.reply_text(t(user.id, "withdraw_wait", time=human_remaining(remain)))
        return

    def _txn(conn, cur):
        cur.execute("UPDATE users SET stars = COALESCE(stars, 0) - %s WHERE user_id = %s", (amount, user.id))
        cur.execute(
            """
            INSERT INTO withdrawals (user_id, amount, amount_stars, status, created_at)
            VALUES (%s, %s, %s, 'pending', %s)
            RETURNING id
            """,
            (user.id, amount, amount, now_iso()),
        )
        row = cur.fetchone()
        return int(row["id"])

    wd_id = transaction(_txn)
    update_withdraw_eligibility_if_needed(user.id)

    text_admin = (
        f"💸 Withdraw Request\n"
        f"ID: #{wd_id}\n"
        f"User: {user.id} (@{user.username or 'no_username'})\n"
        f"Amount: {pretty_amount(amount)}"
    )
    admin_msg = await context.bot.send_message(
        ADMIN_ID,
        text_admin,
        reply_markup=InlineKeyboardMarkup(
            [[
                InlineKeyboardButton("✅ Approve", callback_data=f"wd_approve_{wd_id}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"wd_reject_{wd_id}"),
            ]]
        ),
    )
    try:
        channel_msg = await context.bot.send_message(PAYMENT_CHANNEL, f"⏳ Pending\nUser: {user.id}\nAmount: {pretty_amount(amount)}")
        execute(
            "UPDATE withdrawals SET admin_message_id = %s, channel_message_id = %s WHERE id = %s",
            (admin_msg.message_id, channel_msg.message_id, wd_id),
        )
    except Exception:
        execute(
            "UPDATE withdrawals SET admin_message_id = %s WHERE id = %s",
            (admin_msg.message_id, wd_id),
        )

    await query.message.reply_text(t(user.id, "withdraw_sent"), reply_markup=main_menu(user.id))


async def chat_member_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_member = update.my_chat_member
    if not chat_member:
        return

    chat = chat_member.chat
    new_status = chat_member.new_chat_member.status
    if new_status in ("member", "administrator"):
        save_promo_chat(chat.id, chat.title or "", chat.type)
    elif new_status in ("left", "kicked"):
        deactivate_promo_chat(chat.id)


# =====================================
# MAIN
# =====================================
def main():
    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("users", users_cmd))
    app.add_handler(CommandHandler("refstats", refstats_cmd))
    app.add_handler(CommandHandler("withdraws", withdraws_cmd))
    app.add_handler(CommandHandler("botstats", botstats_cmd))
    app.add_handler(CommandHandler("taskslist", taskslist_cmd))
    app.add_handler(CommandHandler("taskstats", taskstats_cmd))

    app.add_handler(CallbackQueryHandler(lang_callback, pattern=r"^lang_(ps|en)$"))
    app.add_handler(CallbackQueryHandler(withdraw_callback, pattern=r"^withdraw_"))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(ChatMemberHandler(chat_member_handler, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.PHOTO & filters.ChatType.PRIVATE, handle_photo_proof))
    app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.PRIVATE, handle_text))

    if app.job_queue:
        app.job_queue.run_repeating(promo_job, interval=timedelta(hours=PROMO_INTERVAL_HOURS), first=30)
        app.job_queue.run_repeating(leave_check_job, interval=timedelta(hours=LEAVE_CHECK_INTERVAL_HOURS), first=90)

    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
