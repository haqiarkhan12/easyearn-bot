import logging
import os
import re
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.constants import ChatMemberStatus
from telegram.error import BadRequest, Forbidden
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
DAILY_BONUS_STARS = Decimal("1")
WITHDRAW_OPTIONS = [Decimal("15"), Decimal("25"), Decimal("50")]
BONUS_INTERVAL_HOURS = 24
PROMO_INTERVAL_HOURS = 24
LEAVE_CHECK_INTERVAL_HOURS = 2
WITHDRAW_COOLDOWN_HOURS = 4
DEFAULT_PAGE_SIZE = 8

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
@contextmanager
def get_conn():
    conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute(query: str, params: tuple = (), returning: bool = False) -> Optional[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            row = cur.fetchone() if returning else None
            return dict(row) if row else None


def fetch_one(query: str, params: tuple = ()) -> Optional[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            return dict(row) if row else None


def fetch_all(query: str, params: tuple = ()) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]


def safe_exec(query: str):
    try:
        execute(query)
    except Exception as exc:
        logger.info("safe exec skipped: %s", exc)


# =====================================
# UTILS
# =====================================
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().isoformat()


def decimalize(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return Decimal(str(value or 0)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def pretty_amount(value: Any) -> str:
    dec = decimalize(value)
    txt = format(dec.normalize(), "f") if dec != dec.to_integral() else str(dec.quantize(Decimal("1")))
    return txt.rstrip("0").rstrip(".") if "." in txt else txt


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def now_pretty(value: Optional[str] = None) -> str:
    dt = parse_dt(value) or now_utc()
    try:
        return dt.strftime("%-d %b %Y, %-I:%M:%S %p UTC")
    except Exception:
        return dt.strftime("%d %b %Y, %I:%M:%S %p UTC")


def human_remaining(delta: timedelta) -> str:
    total_seconds = max(0, int(delta.total_seconds()))
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    return f"{hours}h {minutes}m"


def extract_chat_username(link_or_username: str) -> Optional[str]:
    value = (link_or_username or "").strip()
    if value.startswith("@") and re.fullmatch(r"@[A-Za-z0-9_]{4,}", value):
        return value
    match = re.search(r"t\.me/([A-Za-z0-9_]{4,})", value)
    if match:
        return "@" + match.group(1)
    return None


def task_url(username_or_link: str) -> str:
    if username_or_link.startswith("http"):
        return username_or_link
    username = extract_chat_username(username_or_link)
    return f"https://t.me/{username[1:]}" if username else username_or_link


def is_private(update: Update) -> bool:
    return bool(update.effective_chat and update.effective_chat.type == "private")


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
        "referral": "👥 ستاسو ریفرل لینک:\n{link}\n\nتاسو به د خپلو ریفرلونو له اعتبار لرونکي عاید څخه 15% ترلاسه کوئ.\nټول ریفرلونه: {count}",
        "tasks_empty": "❌ فعلاً هېڅ تاسک نشته",
        "task_done": "✅ تاسک بشپړ شو\n⭐ {stars}",
        "task_already": "تاسو دا تاسک مخکې بشپړ کړی",
        "task_fail": "❌ لومړی چینل/ګروپ جوین کړئ، بیا تایید وکړئ",
        "task_bot_fail": "❌ د دې bot task دقیق اتومات تایید ممکن نه دی. screenshot proof ولېږئ.",
        "bonus_added": "✅ ورځنی بونس واخیستل شو: {stars} ⭐",
        "bonus_wait": "⏳ بونس مخکې اخیستل شوی. پاتې وخت: {remaining}",
        "withdraw_choose": "💸 د ویډرا لپاره انتخاب وکړئ:",
        "withdraw_low": "❌ بیلانس کم دی",
        "withdraw_cooldown": "⏳ لا ویډرا نه شي کېدای. د انتظار پاتې وخت: {remaining}",
        "admin_low": "❌ د اډمین بیلانس کم دی",
        "about": "ℹ️ زمونږ په اړه\n\nEasyEarn Bot د تاسکونو، ریفرلونو او ورځني بونس له لارې د ستورو ګټلو سیستم دی.",
        "support": "📞 سپورټ\n\nمهرباني وکړئ دې یوزرنیم ته مسج وکړئ:\n{username}",
        "new_task": "📢 نوی تاسک اضافه شو!\n\n{title}\n⭐ انعام: {reward}",
        "stats_admin": "👥 ټول یوزران: {users}\n🆕 د نن یوزران: {today}\n⭐ د ټولو یوزرانو ستوري: {stars}\n⭐ د اډمین ستوري: {admin_stars}\n📝 فعال تاسکونه: {tasks}",
        "admin_only": "دا برخه یوازې اډمین ته ده.",
        "admin_help": "🛠 Admin Commands\n\n/users\n/refstats\n/withdraws\n/botstats\n/taskslist\n/taskstats\n/ban USER_ID [reason]\n/unban USER_ID",
        "broadcast_prompt": "هغه مسج ولیکئ چې ټولو users ته ولاړ شي.",
        "addtask_kind": "د task ډول انتخاب کړئ:",
        "addtask_link": "د چینل/ګروپ لینک یا @username راولېږئ.",
        "addtask_title": "د task عنوان راولېږئ.",
        "addtask_reward": "ریوارډ ولیکئ، مثال: 0.5",
        "addtask_post_link": "د post لینک راولېږئ.",
        "addtask_bot_link": "د bot لینک راولېږئ. مثال: https://t.me/SomeBot?start=abc",
        "addbalance_prompt": "هغه stars ولیکئ چې اډمین بیلانس ته اضافه شي. مثال: 1000",
        "addbalance_done": "✅ اډمین بیلانس {amount} stars سره زیات شو.\n⭐ نوی بیلانس: {new_balance}",
        "removetask_prompt": "د لرې کولو لپاره تاسک انتخاب کړئ.",
        "cancelled": "❌ عمل لغوه شو.",
        "open_task_btn": "🔗 تاسک خلاص کړه",
        "verify_btn": "✅ تایید",
        "send_proof_btn": "📸 proof ولېږه",
        "task_item": "📢 {title}\n⭐ انعام: {stars}",
        "leave_notice": "⚠️ تاسو یو rewarded چینل/ګروپ پرېښود. ستاسو reward بېرته کم شو او task بیا فعال شو.",
        "new_withdraw": "📤 د ویډرا نوې غوښتنه!",
        "proof_prompt": "📸 مهرباني وکړئ screenshot proof همدا اوس راولېږئ.",
        "proof_saved": "✅ proof واستول شو. د اډمین تایید ته منتظر اوسئ.",
        "proof_rejected": "❌ ستاسو proof رد شو. task بیا درته ښکاره شو.",
        "proof_approved": "✅ ستاسو proof منظور شو.\n⭐ {stars}",
        "banned": "⛔ ستاسو اکاونټ بند شوی. د مرستې لپاره سپورټ سره اړیکه ونیسئ.",
        "all_tasks_done": "✅ ټول موجود تاسکونه دې بشپړ کړي",
        "withdraw_sent": "✅ د ویډرا غوښتنه واستول شوه: {amount} ⭐",
        "withdraw_support_no_username": "⚠️ ستا username نشته. د ویډرا لپاره سپورټ سره هم اړیکه ونیسه: {username}",
    },
    "en": {
        "choose_lang": "Choose language:",
        "intro": "Welcome to EasyEarn Bot",
        "force_join": "Please join all required channels first:",
        "joined_btn": "✅ I Joined",
        "join_failed": "Please join all required channels first.",
        "my_stars": "⭐ Your stars: {stars}",
        "referral": "👥 Your referral link:\n{link}\n\nYou earn 15% only from valid completed task rewards of your referrals.\nTotal referrals: {count}",
        "tasks_empty": "❌ No tasks available right now.",
        "task_done": "✅ Task completed\n⭐ {stars}",
        "task_already": "You already completed this task.",
        "task_fail": "❌ Join the channel/group first, then verify.",
        "task_bot_fail": "❌ Exact automatic verification is not possible for this bot task. Please send screenshot proof.",
        "bonus_added": "✅ Daily bonus claimed: {stars} ⭐",
        "bonus_wait": "⏳ Bonus already claimed. Remaining: {remaining}",
        "withdraw_choose": "💸 Choose your withdrawal option:",
        "withdraw_low": "❌ Insufficient balance.",
        "withdraw_cooldown": "⏳ Withdrawal is locked for now. Remaining wait: {remaining}",
        "admin_low": "❌ Admin balance is low.",
        "about": "ℹ️ About Us\n\nEasyEarn Bot is a stars earning system through tasks, referrals, and daily bonus.",
        "support": "📞 Support\n\nPlease message:\n{username}",
        "new_task": "📢 New task added!\n\n{title}\n⭐ Reward: {reward}",
        "stats_admin": "👥 Total users: {users}\n🆕 Today users: {today}\n⭐ Total user stars: {stars}\n⭐ Admin stars: {admin_stars}\n📝 Active tasks: {tasks}",
        "admin_only": "This section is admin only.",
        "admin_help": "🛠 Admin Commands\n\n/users\n/refstats\n/withdraws\n/botstats\n/taskslist\n/taskstats\n/ban USER_ID [reason]\n/unban USER_ID",
        "broadcast_prompt": "Send the message you want to broadcast.",
        "addtask_kind": "Choose task type:",
        "addtask_link": "Send channel/group link or @username.",
        "addtask_title": "Send task title.",
        "addtask_reward": "Send reward, example: 0.5",
        "addtask_post_link": "Send the post link.",
        "addtask_bot_link": "Send the bot link. Example: https://t.me/SomeBot?start=abc",
        "addbalance_prompt": "Send stars amount to add to admin balance. Example: 1000",
        "addbalance_done": "✅ Admin balance increased by {amount} stars.\n⭐ New balance: {new_balance}",
        "removetask_prompt": "Choose a task to remove.",
        "cancelled": "❌ Action cancelled.",
        "open_task_btn": "🔗 Open Task",
        "verify_btn": "✅ Verify",
        "send_proof_btn": "📸 Send proof",
        "task_item": "📢 {title}\n⭐ Reward: {stars}",
        "leave_notice": "⚠️ You left a rewarded channel/group. Your reward was deducted and the task became active again.",
        "new_withdraw": "📤 New withdrawal request!",
        "proof_prompt": "📸 Please send screenshot proof now.",
        "proof_saved": "✅ Proof received. Waiting for admin review.",
        "proof_rejected": "❌ Your proof was rejected. The task is available again.",
        "proof_approved": "✅ Your proof was approved.\n⭐ {stars}",
        "banned": "⛔ Your account is banned. Contact support for help.",
        "all_tasks_done": "✅ You have completed all available tasks",
        "withdraw_sent": "✅ Withdrawal request sent: {amount} ⭐",
        "withdraw_support_no_username": "⚠️ You do not have a username. Also contact support for withdrawal: {username}",
    },
}


def get_text(lang: str, key: str, **kwargs) -> str:
    return TEXTS[lang][key].format(**kwargs)


# =====================================
# DB INIT / MIGRATIONS
# =====================================
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
            created_at TEXT,
            withdraw_eligible_at TEXT,
            is_banned BOOLEAN DEFAULT FALSE,
            banned_at TEXT,
            ban_reason TEXT,
            last_task_message_id BIGINT,
            last_task_chat_id BIGINT
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
            post_link TEXT,
            bot_link TEXT,
            metadata TEXT
        )
        """
    )
    execute(
        """
        CREATE TABLE IF NOT EXISTS user_tasks (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            task_id INTEGER NOT NULL,
            rewarded_stars NUMERIC(12,2) DEFAULT 0,
            reward_removed INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending',
            created_at TEXT,
            completed_at TEXT,
            last_checked_at TEXT,
            proof_file_id TEXT,
            proof_file_unique_id TEXT,
            proof_message_id BIGINT,
            admin_review_message_id BIGINT,
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
            rejected_at TEXT,
            reason TEXT
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
    execute(
        """
        CREATE TABLE IF NOT EXISTS referral_earnings (
            id SERIAL PRIMARY KEY,
            referrer_id BIGINT NOT NULL,
            referred_user_id BIGINT NOT NULL,
            task_id INTEGER NOT NULL,
            base_reward NUMERIC(12,2) NOT NULL,
            bonus_amount NUMERIC(12,2) NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(referred_user_id, task_id)
        )
        """
    )

    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS withdraw_eligible_at TEXT")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS is_banned BOOLEAN DEFAULT FALSE")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS banned_at TEXT")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS ban_reason TEXT")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_task_message_id BIGINT")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_task_chat_id BIGINT")

    safe_exec("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS task_type TEXT DEFAULT 'channel'")
    safe_exec("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS requires_proof BOOLEAN DEFAULT FALSE")
    safe_exec("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS post_link TEXT")
    safe_exec("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS bot_link TEXT")
    safe_exec("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS metadata TEXT")

    safe_exec("ALTER TABLE user_tasks ADD COLUMN IF NOT EXISTS completed_at TEXT")
    safe_exec("ALTER TABLE user_tasks ADD COLUMN IF NOT EXISTS proof_file_id TEXT")
    safe_exec("ALTER TABLE user_tasks ADD COLUMN IF NOT EXISTS proof_file_unique_id TEXT")
    safe_exec("ALTER TABLE user_tasks ADD COLUMN IF NOT EXISTS proof_message_id BIGINT")
    safe_exec("ALTER TABLE user_tasks ADD COLUMN IF NOT EXISTS admin_review_message_id BIGINT")
    safe_exec("ALTER TABLE user_tasks ADD COLUMN IF NOT EXISTS rejection_reason TEXT")
    safe_exec("ALTER TABLE user_tasks ADD COLUMN IF NOT EXISTS suspicious INTEGER DEFAULT 0")

    safe_exec("ALTER TABLE withdrawals ADD COLUMN IF NOT EXISTS reason TEXT")

    safe_exec("CREATE INDEX IF NOT EXISTS idx_users_referrer_id ON users(referrer_id)")
    safe_exec("CREATE INDEX IF NOT EXISTS idx_users_is_banned ON users(is_banned)")
    safe_exec("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")
    safe_exec("CREATE INDEX IF NOT EXISTS idx_tasks_type ON tasks(task_type)")
    safe_exec("CREATE INDEX IF NOT EXISTS idx_user_tasks_user ON user_tasks(user_id)")
    safe_exec("CREATE INDEX IF NOT EXISTS idx_user_tasks_task ON user_tasks(task_id)")
    safe_exec("CREATE INDEX IF NOT EXISTS idx_user_tasks_status ON user_tasks(status)")
    safe_exec("CREATE INDEX IF NOT EXISTS idx_withdrawals_status ON withdrawals(status)")

    admin = fetch_one("SELECT user_id, stars FROM users WHERE user_id = %s", (ADMIN_ID,))
    if not admin:
        execute(
            """
            INSERT INTO users (user_id, username, full_name, lang, stars, created_at)
            VALUES (%s, %s, %s, 'ps', %s, %s)
            """,
            (ADMIN_ID, "admin", "Admin", ADMIN_START_STARS, now_iso()),
        )
    else:
        execute("UPDATE users SET stars = %s WHERE user_id = %s", (ADMIN_START_STARS, ADMIN_ID))


# =====================================
# USER / TASK HELPERS
# =====================================
def ensure_user(user_id: int, username: str | None, full_name: str | None) -> None:
    row = fetch_one("SELECT user_id FROM users WHERE user_id = %s", (user_id,))
    if not row:
        execute(
            """
            INSERT INTO users (user_id, username, full_name, created_at)
            VALUES (%s, %s, %s, %s)
            """,
            (user_id, username or "", full_name or "", now_iso()),
        )
    else:
        execute(
            "UPDATE users SET username = %s, full_name = %s WHERE user_id = %s",
            (username or "", full_name or "", user_id),
        )


def get_user(user_id: int) -> Optional[dict]:
    return fetch_one("SELECT * FROM users WHERE user_id = %s", (user_id,))


def get_lang(user_id: int) -> str:
    row = get_user(user_id)
    lang = (row or {}).get("lang") or "ps"
    return lang if lang in ("ps", "en") else "ps"


def t(user_id: int, key: str, **kwargs) -> str:
    return get_text(get_lang(user_id), key, **kwargs)


def set_lang(user_id: int, lang: str) -> None:
    execute("UPDATE users SET lang = %s WHERE user_id = %s", (lang, user_id))


def is_banned(user_id: int) -> bool:
    row = fetch_one("SELECT is_banned FROM users WHERE user_id = %s", (user_id,))
    return bool(row and row.get("is_banned"))


def get_stars(user_id: int) -> Decimal:
    row = fetch_one("SELECT stars FROM users WHERE user_id = %s", (user_id,))
    return decimalize((row or {}).get("stars") or 0)


def update_withdraw_eligibility(user_id: int, conn=None) -> None:
    min_withdraw = min(WITHDRAW_OPTIONS)
    owns_conn = conn is None
    if owns_conn:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT stars, withdraw_eligible_at FROM users WHERE user_id = %s FOR UPDATE", (user_id,))
            row = cur.fetchone()
            if not row:
                return
            stars = decimalize(row["stars"])
            eligible_at = row.get("withdraw_eligible_at")
            if stars >= min_withdraw and not eligible_at:
                cur.execute("UPDATE users SET withdraw_eligible_at = %s WHERE user_id = %s", (now_iso(), user_id))
            elif stars < min_withdraw and eligible_at:
                cur.execute("UPDATE users SET withdraw_eligible_at = NULL WHERE user_id = %s", (user_id,))
        if owns_conn:
            conn.commit()
    finally:
        if owns_conn:
            conn.close()


def add_stars(user_id: int, amount: Decimal, conn=None) -> None:
    amount = decimalize(amount)
    owns_conn = conn is None
    if owns_conn:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET stars = COALESCE(stars, 0) + %s WHERE user_id = %s",
                (amount, user_id),
            )
        update_withdraw_eligibility(user_id, conn=conn)
        if owns_conn:
            conn.commit()
    finally:
        if owns_conn:
            conn.close()


def referral_link(user_id: int) -> str:
    return f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"


def referral_count(user_id: int) -> int:
    row = fetch_one("SELECT COUNT(*) AS c FROM users WHERE referrer_id = %s", (user_id,))
    return int((row or {}).get("c") or 0)


def get_user_refs(user_id: int) -> list[dict]:
    return fetch_all(
        "SELECT user_id AS id, username, full_name, created_at FROM users WHERE referrer_id = %s ORDER BY created_at ASC",
        (user_id,),
    )


def top_referrals(limit: int = 50) -> list[dict]:
    return fetch_all(
        """
        SELECT referrer_id, COUNT(*) AS refs
        FROM users
        WHERE referrer_id IS NOT NULL
        GROUP BY referrer_id
        ORDER BY refs DESC, referrer_id ASC
        LIMIT %s
        """,
        (limit,),
    )


def get_task(task_id: int) -> Optional[dict]:
    return fetch_one("SELECT * FROM tasks WHERE id = %s", (task_id,))


def get_task_completion(user_id: int, task_id: int) -> Optional[dict]:
    return fetch_one(
        "SELECT * FROM user_tasks WHERE user_id = %s AND task_id = %s",
        (user_id, task_id),
    )


def get_visible_tasks_for_user(user_id: int) -> list[dict]:
    return fetch_all(
        """
        SELECT t.*
        FROM tasks t
        WHERE t.status = 'active'
          AND NOT EXISTS (
              SELECT 1
              FROM user_tasks ut
              WHERE ut.user_id = %s
                AND ut.task_id = t.id
                AND ut.status = 'completed'
                AND ut.reward_removed = 0
          )
        ORDER BY t.id DESC
        """,
        (user_id,),
    )


def set_last_task_message(user_id: int, chat_id: int, message_id: int) -> None:
    execute(
        "UPDATE users SET last_task_chat_id = %s, last_task_message_id = %s WHERE user_id = %s",
        (chat_id, message_id, user_id),
    )


def clear_last_task_message(user_id: int) -> None:
    execute(
        "UPDATE users SET last_task_chat_id = NULL, last_task_message_id = NULL WHERE user_id = %s",
        (user_id,),
    )


def task_stats_rows(limit: int = 100) -> list[dict]:
    return fetch_all(
        """
        SELECT
            t.id,
            t.channel_title,
            t.reward_stars,
            t.status,
            t.task_type,
            t.created_at,
            COALESCE(SUM(CASE WHEN ut.status = 'completed' AND ut.reward_removed = 0 THEN 1 ELSE 0 END), 0) AS join_count
        FROM tasks t
        LEFT JOIN user_tasks ut ON ut.task_id = t.id
        WHERE t.status = 'active'
        GROUP BY t.id, t.channel_title, t.reward_stars, t.status, t.task_type, t.created_at
        ORDER BY t.id DESC
        LIMIT %s
        """,
        (limit,),
    )


def active_task_rows(limit: int = 100) -> list[dict]:
    return fetch_all(
        "SELECT id, channel_title, reward_stars, status, task_type, created_at FROM tasks WHERE status = 'active' ORDER BY id DESC LIMIT %s",
        (limit,),
    )


def record_referral_bonus_if_needed(referred_user_id: int, task_id: int, reward: Decimal, conn) -> None:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT referrer_id FROM users WHERE user_id = %s", (referred_user_id,))
        row = cur.fetchone()
        if not row or not row.get("referrer_id"):
            return
        referrer_id = int(row["referrer_id"])
        bonus_amount = decimalize((reward * REFERRAL_PERCENT) / Decimal("100"))
        if bonus_amount <= 0:
            return
        cur.execute(
            """
            INSERT INTO referral_earnings (referrer_id, referred_user_id, task_id, base_reward, bonus_amount, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (referred_user_id, task_id) DO NOTHING
            RETURNING id
            """,
            (referrer_id, referred_user_id, task_id, reward, bonus_amount, now_iso()),
        )
        created = cur.fetchone()
        if created:
            cur.execute(
                "UPDATE users SET stars = COALESCE(stars, 0) + %s WHERE user_id = %s",
                (bonus_amount, referrer_id),
            )
            update_withdraw_eligibility(referrer_id, conn=conn)


def complete_exact_task_reward(user_id: int, task_id: int, reward: Decimal) -> tuple[bool, str]:
    reward = decimalize(reward)
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT status, reward_removed FROM user_tasks WHERE user_id = %s AND task_id = %s FOR UPDATE", (user_id, task_id))
            existing = cur.fetchone()
            if existing and existing["status"] == "completed" and int(existing["reward_removed"] or 0) == 0:
                return False, "already"

            cur.execute("SELECT stars FROM users WHERE user_id = %s FOR UPDATE", (ADMIN_ID,))
            admin = cur.fetchone()
            admin_stars = decimalize((admin or {}).get("stars") or 0)
            if admin_stars < reward:
                return False, "admin_low"

            cur.execute("SELECT stars FROM users WHERE user_id = %s FOR UPDATE", (user_id,))
            cur.fetchone()

            cur.execute("UPDATE users SET stars = COALESCE(stars, 0) - %s WHERE user_id = %s", (reward, ADMIN_ID))
            cur.execute("UPDATE users SET stars = COALESCE(stars, 0) + %s WHERE user_id = %s", (reward, user_id))

            if existing:
                cur.execute(
                    """
                    UPDATE user_tasks
                    SET rewarded_stars = %s,
                        reward_removed = 0,
                        status = 'completed',
                        completed_at = %s,
                        last_checked_at = %s,
                        rejection_reason = NULL,
                        suspicious = 0
                    WHERE user_id = %s AND task_id = %s
                    """,
                    (reward, now_iso(), now_iso(), user_id, task_id),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO user_tasks
                        (user_id, task_id, rewarded_stars, reward_removed, status, created_at, completed_at, last_checked_at)
                    VALUES (%s, %s, %s, 0, 'completed', %s, %s, %s)
                    ON CONFLICT (user_id, task_id)
                    DO UPDATE SET rewarded_stars = EXCLUDED.rewarded_stars,
                                  reward_removed = 0,
                                  status = 'completed',
                                  completed_at = EXCLUDED.completed_at,
                                  last_checked_at = EXCLUDED.last_checked_at,
                                  suspicious = 0
                    """,
                    (user_id, task_id, reward, now_iso(), now_iso(), now_iso()),
                )

            update_withdraw_eligibility(user_id, conn=conn)
            update_withdraw_eligibility(ADMIN_ID, conn=conn)
            record_referral_bonus_if_needed(user_id, task_id, reward, conn)
    return True, "ok"


def mark_proof_pending(user_id: int, task_id: int, photo_file_id: str, photo_unique_id: str, proof_message_id: int) -> None:
    execute(
        """
        INSERT INTO user_tasks
            (user_id, task_id, rewarded_stars, reward_removed, status, created_at, last_checked_at,
             proof_file_id, proof_file_unique_id, proof_message_id, suspicious)
        VALUES (%s, %s, 0, 1, 'pending_review', %s, %s, %s, %s, %s, 0)
        ON CONFLICT (user_id, task_id)
        DO UPDATE SET status = 'pending_review',
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
            (task_type, channel_title, chat_username, link, reward_stars, status, created_at, requires_proof, post_link, bot_link)
        VALUES (%s, %s, %s, %s, %s, 'active', %s, %s, %s, %s)
        RETURNING id
        """,
        (task_type, channel_title, chat_username, link, reward_stars, now_iso(), requires_proof, post_link, bot_link),
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


def cancel_reply_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)


def lang_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🇦🇫 پښتو", callback_data="lang_ps")],
            [InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
            [InlineKeyboardButton("⬅️ Back", callback_data="back_main")],
        ]
    )


def force_join_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(f"📢 {username}", url=link)] for username, link in FORCE_JOIN_CHANNELS]
    rows.append([InlineKeyboardButton(t(user_id, "joined_btn"), callback_data="check_force_join")])
    return InlineKeyboardMarkup(rows)


def withdraw_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(f"⭐ {pretty_amount(v)} Stars", callback_data=f"withdraw_{pretty_amount(v)}")] for v in WITHDRAW_OPTIONS]
    rows.append([InlineKeyboardButton("⬅️ Back" if get_lang(user_id) == "en" else "⬅️ شاته", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)


def task_list_keyboard(user_id: int, tasks: list[dict], page: int = 0) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    start = page * DEFAULT_PAGE_SIZE
    items = tasks[start : start + DEFAULT_PAGE_SIZE]
    lang = get_lang(user_id)
    for item in items:
        task_id = int(item["id"])
        rows.append([InlineKeyboardButton(f"{task_id}. {item['channel_title']}", callback_data=f"task_open_{task_id}_{page}")])
    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"tasks_page_{page - 1}"))
    if start + DEFAULT_PAGE_SIZE < len(tasks):
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"tasks_page_{page + 1}"))
    if nav_row:
        rows.append(nav_row)
    rows.append([InlineKeyboardButton("⬅️ Back" if lang == "en" else "⬅️ شاته", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)


def single_task_keyboard(user_id: int, task: dict, page: int = 0) -> InlineKeyboardMarkup:
    lang = get_lang(user_id)
    rows: list[list[InlineKeyboardButton]] = []
    open_link = task.get("post_link") or task.get("bot_link") or task.get("link")
    if open_link:
        rows.append([InlineKeyboardButton(t(user_id, "open_task_btn"), url=open_link)])
    task_type = task.get("task_type")
    if task_type in ("reaction", "bot_link", "facebook", "youtube") or task.get("requires_proof"):
        rows.append([InlineKeyboardButton(t(user_id, "send_proof_btn"), callback_data=f"proof_{task['id']}_{page}")])
    else:
        rows.append([InlineKeyboardButton(t(user_id, "verify_btn"), callback_data=f"verify_{task['id']}_{page}")])
    rows.append([InlineKeyboardButton("⬅️ Back" if lang == "en" else "⬅️ شاته", callback_data=f"tasks_page_{page}")])
    return InlineKeyboardMarkup(rows)


def add_task_kind_keyboard(user_id: int) -> InlineKeyboardMarkup:
    lang = get_lang(user_id)
    rows = [
        [InlineKeyboardButton("📢 Channel / Group", callback_data="admin_add_kind_channel")],
        [InlineKeyboardButton("👍 Reaction", callback_data="admin_add_kind_reaction")],
        [InlineKeyboardButton("🤖 Bot Link", callback_data="admin_add_kind_botlink")],
        [InlineKeyboardButton("▶️ YouTube", callback_data="admin_add_kind_youtube")],
        [InlineKeyboardButton("📘 Facebook", callback_data="admin_add_kind_facebook")],
        [InlineKeyboardButton("⬅️ Back" if lang == "en" else "⬅️ شاته", callback_data="back_main")],
    ]
    return InlineKeyboardMarkup(rows)


def proof_review_keyboard(record_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("✅ Approve", callback_data=f"proof_ok_{record_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"proof_no_{record_id}"),
        ]]
    )


# =====================================
# TELEGRAM HELPERS
# =====================================
async def safe_delete_message(bot, chat_id: int, message_id: Optional[int]) -> None:
    if not message_id:
        return
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


async def maybe_cleanup_old_task_message(bot, user_id: int) -> None:
    user = get_user(user_id)
    if not user:
        return
    await safe_delete_message(bot, user.get("last_task_chat_id"), user.get("last_task_message_id"))
    clear_last_task_message(user_id)


async def is_bot_admin_in_chat(bot, chat_username: str) -> bool:
    try:
        me = await bot.get_me()
        member = await bot.get_chat_member(chat_id=chat_username, user_id=me.id)
        return member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER, "creator")
    except Exception:
        return False


async def check_join(bot, chat_username: str, user_id: int) -> tuple[bool, str]:
    try:
        if not await is_bot_admin_in_chat(bot, chat_username):
            return False, "bot_not_admin"
        member = await bot.get_chat_member(chat_id=chat_username, user_id=user_id)
        if member.status in (
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER,
            "member",
            "administrator",
            "creator",
            "owner",
        ):
            return True, "joined"
        return False, "not_joined"
    except Forbidden:
        return False, "bot_not_admin"
    except BadRequest:
        return False, "not_found"
    except Exception:
        return False, "error"


async def check_force_join_all(bot, user_id: int) -> bool:
    for username, _ in FORCE_JOIN_CHANNELS:
        joined, _ = await check_join(bot, username, user_id)
        if not joined:
            return False
    return True


async def notify_all_users(context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
    users = fetch_all("SELECT user_id FROM users WHERE COALESCE(is_banned, FALSE) = FALSE")
    for row in users:
        try:
            await context.bot.send_message(chat_id=row["user_id"], text=text)
        except Exception:
            continue


def render_task_summary(user_id: int, task: dict) -> str:
    lang = get_lang(user_id)
    reward = pretty_amount(task["reward_stars"])
    task_type = task.get("task_type") or "channel"
    if lang == "ps":
        type_text = {
            "channel": "چینل/ګروپ",
            "reaction": "رییکشن",
            "bot_link": "بوټ لینک",
        }.get(task_type, task_type)
        return f"📢 {task['channel_title']}\n⭐ انعام: {reward}\n🧩 ډول: {type_text}"
    return f"📢 {task['channel_title']}\n⭐ Reward: {reward}\n🧩 Type: {task_type}"


async def send_task_list(update_or_query_message, bot, user_id: int, page: int = 0):
    tasks = get_visible_tasks_for_user(user_id)
    await maybe_cleanup_old_task_message(bot, user_id)
    if not tasks:
        sent = await update_or_query_message.reply_text(t(user_id, "all_tasks_done"), reply_markup=main_menu(user_id))
        set_last_task_message(user_id, sent.chat_id, sent.message_id)
        return

    lang = get_lang(user_id)
    title = "📋 Available Tasks" if lang == "en" else "📋 موجود تاسکونه"
    sent = await update_or_query_message.reply_text(
        title,
        reply_markup=task_list_keyboard(user_id, tasks, page=page),
    )
    set_last_task_message(user_id, sent.chat_id, sent.message_id)


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
        execute(
            """
            INSERT INTO promo_chats (chat_id, title, chat_type, is_active, created_at)
            VALUES (%s, %s, %s, 1, %s)
            ON CONFLICT (chat_id)
            DO UPDATE SET title = EXCLUDED.title, chat_type = EXCLUDED.chat_type, is_active = 1
            """,
            (chat.id, title, chat.type, now_iso()),
        )
    elif status in ("left", "kicked"):
        execute("UPDATE promo_chats SET is_active = 0 WHERE chat_id = %s", (chat.id,))


async def daily_promo_post(context: ContextTypes.DEFAULT_TYPE):
    chats = fetch_all("SELECT chat_id FROM promo_chats WHERE is_active = 1")
    for row in chats:
        try:
            await context.bot.send_message(chat_id=row["chat_id"], text=PROMO_TEXT)
        except Exception as exc:
            logger.info("promo failed for %s: %s", row["chat_id"], exc)


# =====================================
# PENALTIES / SECURITY
# =====================================
async def process_leave_penalties_for_user(bot, user_id: int):
    rows = fetch_all(
        """
        SELECT ut.id, ut.task_id, ut.rewarded_stars, t.chat_username, t.task_type
        FROM user_tasks ut
        JOIN tasks t ON ut.task_id = t.id
        WHERE ut.user_id = %s
          AND ut.status = 'completed'
          AND ut.reward_removed = 0
          AND t.status = 'active'
          AND t.task_type = 'channel'
        """,
        (user_id,),
    )
    if not rows:
        return

    for row in rows:
        joined, _ = await check_join(bot, row["chat_username"], user_id)
        if joined:
            execute(
                "UPDATE user_tasks SET last_checked_at = %s WHERE id = %s",
                (now_iso(), row["id"]),
            )
            continue

        reward = decimalize(row["rewarded_stars"])
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT reward_removed FROM user_tasks WHERE id = %s FOR UPDATE", (row["id"],))
                current = cur.fetchone()
                if not current or int(current.get("reward_removed") or 0) == 1:
                    continue
                cur.execute("UPDATE users SET stars = COALESCE(stars, 0) - %s WHERE user_id = %s", (reward, user_id))
                cur.execute(
                    """
                    UPDATE user_tasks
                    SET reward_removed = 1,
                        status = 'left',
                        last_checked_at = %s
                    WHERE id = %s
                    """,
                    (now_iso(), row["id"]),
                )
                update_withdraw_eligibility(user_id, conn=conn)
        try:
            await bot.send_message(chat_id=user_id, text=t(user_id, "leave_notice"), reply_markup=main_menu(user_id))
        except Exception:
            pass


async def periodic_leave_check(context: ContextTypes.DEFAULT_TYPE):
    users = fetch_all(
        """
        SELECT DISTINCT ut.user_id
        FROM user_tasks ut
        JOIN tasks t ON t.id = ut.task_id
        WHERE ut.status = 'completed' AND ut.reward_removed = 0 AND t.task_type = 'channel' AND t.status = 'active'
        """
    )
    for row in users:
        try:
            await process_leave_penalties_for_user(context.bot, int(row["user_id"]))
        except Exception as exc:
            logger.info("leave check failed for %s: %s", row["user_id"], exc)


# =====================================
# START / GENERAL FLOW
# =====================================
async def guard_user_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    if not user:
        return False
    ensure_user(user.id, user.username or "", user.full_name or "")
    if is_banned(user.id):
        if update.message:
            await update.message.reply_text(t(user.id, "banned"))
        elif update.callback_query:
            await update.callback_query.answer(t(user.id, "banned"), show_alert=True)
        return False
    if not await check_force_join_all(context.bot, user.id):
        if update.message:
            await update.message.reply_text(t(user.id, "force_join"), reply_markup=force_join_keyboard(user.id))
        elif update.callback_query:
            await update.callback_query.message.reply_text(t(user.id, "force_join"), reply_markup=force_join_keyboard(user.id))
        return False
    return True


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    user = update.effective_user
    ensure_user(user.id, user.username or "", user.full_name or "")

    referrer_id = None
    txt = (update.message.text or "").strip()
    if txt.startswith("/start ref_"):
        try:
            referrer_id = int(txt.split("ref_")[1].strip())
            if referrer_id == user.id:
                referrer_id = None
        except Exception:
            referrer_id = None

    row = get_user(user.id)
    if referrer_id and row and not row.get("referrer_id"):
        execute(
            "UPDATE users SET referrer_id = %s WHERE user_id = %s AND (referrer_id IS NULL OR referrer_id = 0)",
            (referrer_id, user.id),
        )

    context.user_data.pop("admin_flow", None)
    context.user_data.pop("awaiting_proof_task_id", None)

    if not await guard_user_access(update, context):
        return

    await process_leave_penalties_for_user(context.bot, user.id)
    await update.message.reply_text(t(user.id, "intro"), reply_markup=main_menu(user.id))


# =====================================
# CALLBACKS
# =====================================
async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    data = query.data or ""

    if is_banned(user.id):
        await query.answer(t(user.id, "banned"), show_alert=True)
        return

    if data == "check_force_join":
        if await check_force_join_all(context.bot, user.id):
            await query.message.reply_text(
                "✅ Access granted." if get_lang(user.id) == "en" else "✅ لاسرسی درکړل شو",
                reply_markup=main_menu(user.id),
            )
        else:
            await query.message.reply_text(t(user.id, "join_failed"), reply_markup=force_join_keyboard(user.id))
        return

    if data == "back_main":
        await maybe_cleanup_old_task_message(context.bot, user.id)
        await query.message.reply_text(t(user.id, "intro"), reply_markup=main_menu(user.id))
        return

    if data == "lang_en":
        set_lang(user.id, "en")
        await query.message.reply_text("✅ Language changed to English", reply_markup=main_menu(user.id))
        return

    if data == "lang_ps":
        set_lang(user.id, "ps")
        await query.message.reply_text("✅ ژبه پښتو ته بدله شوه", reply_markup=main_menu(user.id))
        return

    if data.startswith("tasks_page_"):
        if not await guard_user_access(update, context):
            return
        page = int(data.split("_")[-1])
        await send_task_list(query.message, context.bot, user.id, page=page)
        return

    if data.startswith("task_open_"):
        if not await guard_user_access(update, context):
            return
        _, _, task_id, page = data.split("_")
        task = get_task(int(task_id))
        if not task or task.get("status") != "active":
            await query.message.reply_text("Task not found.", reply_markup=main_menu(user.id))
            return
        await maybe_cleanup_old_task_message(context.bot, user.id)
        sent = await query.message.reply_text(
            render_task_summary(user.id, task),
            reply_markup=single_task_keyboard(user.id, task, page=int(page)),
        )
        set_last_task_message(user.id, sent.chat_id, sent.message_id)
        return

    if data.startswith("verify_"):
        if not await guard_user_access(update, context):
            return
        _, task_id, page = data.split("_")
        task = fetch_one("SELECT * FROM tasks WHERE id = %s AND status = 'active'", (int(task_id),))
        if not task:
            await query.message.reply_text("Task not found.", reply_markup=main_menu(user.id))
            return

        completion = get_task_completion(user.id, int(task_id))
        if completion and completion.get("status") == "completed" and int(completion.get("reward_removed") or 0) == 0:
            await query.message.reply_text(t(user.id, "task_already"), reply_markup=main_menu(user.id))
            return

        if task.get("task_type") != "channel":
            await query.message.reply_text(t(user.id, "task_bot_fail"), reply_markup=main_menu(user.id))
            return

        joined, reason = await check_join(context.bot, task["chat_username"], user.id)
        if not joined:
            if reason == "bot_not_admin":
                await query.message.reply_text(
                    "❌ Bot is not admin in the channel/group." if get_lang(user.id) == "en" else "❌ بوټ په چینل/ګروپ کې اډمین نه دی.",
                    reply_markup=main_menu(user.id),
                )
            else:
                await query.message.reply_text(t(user.id, "task_fail"), reply_markup=main_menu(user.id))
            return

        ok, status = complete_exact_task_reward(user.id, int(task_id), decimalize(task["reward_stars"]))
        if not ok:
            if status == "already":
                await query.message.reply_text(t(user.id, "task_already"), reply_markup=main_menu(user.id))
            else:
                await query.message.reply_text(t(user.id, "admin_low"), reply_markup=main_menu(user.id))
            return

        await maybe_cleanup_old_task_message(context.bot, user.id)
        try:
            await query.message.delete()
        except Exception:
            pass
        await query.message.reply_text(
            t(user.id, "task_done", stars=pretty_amount(task["reward_stars"])),
            reply_markup=main_menu(user.id),
        )
        await send_task_list(query.message, context.bot, user.id, page=int(page))
        return

    if data.startswith("proof_"):
        if not await guard_user_access(update, context):
            return
        _, task_id, page = data.split("_")
        task = get_task(int(task_id))
        if not task or task.get("status") != "active":
            await query.message.reply_text("Task not found.", reply_markup=main_menu(user.id))
            return
        context.user_data["awaiting_proof_task_id"] = int(task_id)
        context.user_data["awaiting_proof_page"] = int(page)
        await query.message.reply_text(t(user.id, "proof_prompt"), reply_markup=main_menu(user.id))
        return

    if data.startswith("withdraw_"):
        if not await guard_user_access(update, context):
            return
        amount = decimalize(data.split("_")[-1])
        await process_leave_penalties_for_user(context.bot, user.id)
        stars = get_stars(user.id)
        if stars < amount:
            await query.message.reply_text(t(user.id, "withdraw_low"), reply_markup=main_menu(user.id))
            return
        remaining = withdraw_cooldown_remaining(user.id)
        if remaining and remaining.total_seconds() > 0:
            await query.message.reply_text(
                t(user.id, "withdraw_cooldown", remaining=human_remaining(remaining)),
                reply_markup=main_menu(user.id),
            )
            return

        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT stars FROM users WHERE user_id = %s FOR UPDATE", (user.id,))
                row = cur.fetchone()
                fresh_stars = decimalize((row or {}).get("stars") or 0)
                if fresh_stars < amount:
                    await query.message.reply_text(t(user.id, "withdraw_low"), reply_markup=main_menu(user.id))
                    return
                cur.execute("UPDATE users SET stars = COALESCE(stars, 0) - %s WHERE user_id = %s", (amount, user.id))
                cur.execute(
                    """
                    INSERT INTO withdrawals (user_id, amount, amount_stars, status, created_at)
                    VALUES (%s, %s, %s, 'pending', %s)
                    RETURNING id
                    """,
                    (user.id, amount, amount, now_iso()),
                )
                wd_id = int(cur.fetchone()["id"])
                update_withdraw_eligibility(user.id, conn=conn)

        wd_user = get_user(user.id)
        username = f"@{wd_user['username']}" if wd_user and wd_user.get("username") else "No username"
        msg = (
            f"{t(user.id, 'new_withdraw')}\n\n"
            f"👤 User: {username}\n"
            f"🆔 UserID: {user.id}\n"
            f"💰 Amount: {pretty_amount(amount)} ⭐\n"
            f"🕒 Time: {now_pretty()}\n"
            f"⏳ Status: Pending"
        )
        admin_kb = InlineKeyboardMarkup(
            [[
                InlineKeyboardButton("✅ Approve", callback_data=f"admin_wd_ok_{wd_id}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"admin_wd_no_{wd_id}"),
            ]]
        )
        try:
            sent = await context.bot.send_message(ADMIN_ID, msg, reply_markup=admin_kb)
            execute("UPDATE withdrawals SET admin_message_id = %s WHERE id = %s", (sent.message_id, wd_id))
        except Exception:
            pass
        try:
            sent = await context.bot.send_message(PAYMENT_CHANNEL, msg)
            execute("UPDATE withdrawals SET channel_message_id = %s WHERE id = %s", (sent.message_id, wd_id))
        except Exception:
            pass

        await query.message.reply_text(t(user.id, "withdraw_sent", amount=pretty_amount(amount)), reply_markup=main_menu(user.id))
        if not (wd_user and wd_user.get("username")):
            await query.message.reply_text(
                t(user.id, "withdraw_support_no_username", username=SUPPORT_USERNAME),
                reply_markup=main_menu(user.id),
            )
        return

    if data.startswith("admin_wd_ok_"):
        if user.id != ADMIN_ID:
            return
        wd_id = int(data.split("_")[-1])
        wd = fetch_one("SELECT * FROM withdrawals WHERE id = %s", (wd_id,))
        if not wd or wd["status"] != "pending":
            return
        execute("UPDATE withdrawals SET status = 'approved', approved_at = %s WHERE id = %s", (now_iso(), wd_id))
        wd_user = get_user(int(wd["user_id"]))
        username = f"@{wd_user['username']}" if wd_user and wd_user.get("username") else "No username"
        updated_text = (
            "📤 New Withdrawal Request!\n\n"
            f"👤 User: {username}\n"
            f"🆔 UserID: {wd['user_id']}\n"
            f"💰 Amount: {pretty_amount(wd['amount_stars'])} ⭐\n"
            f"🕒 Time: {now_pretty(wd.get('created_at'))}\n"
            "✅ Status: Approved"
        )
        try:
            if wd.get("channel_message_id"):
                await context.bot.edit_message_text(chat_id=PAYMENT_CHANNEL, message_id=wd["channel_message_id"], text=updated_text)
        except Exception:
            pass
        try:
            if wd.get("admin_message_id"):
                await context.bot.edit_message_text(chat_id=ADMIN_ID, message_id=wd["admin_message_id"], text=updated_text)
        except Exception:
            pass
        try:
            await context.bot.send_message(int(wd["user_id"]), f"✅ Your withdrawal has been approved: {pretty_amount(wd['amount_stars'])} ⭐", reply_markup=main_menu(int(wd["user_id"])))
        except Exception:
            pass
        await query.message.reply_text("✅ Withdrawal approved.")
        return

    if data.startswith("admin_wd_no_"):
        if user.id != ADMIN_ID:
            return
        wd_id = int(data.split("_")[-1])
        wd = fetch_one("SELECT * FROM withdrawals WHERE id = %s", (wd_id,))
        if not wd or wd["status"] != "pending":
            return
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET stars = COALESCE(stars, 0) + %s WHERE user_id = %s", (decimalize(wd["amount_stars"]), int(wd["user_id"])))
                cur.execute("UPDATE withdrawals SET status = 'rejected', rejected_at = %s WHERE id = %s", (now_iso(), wd_id))
                update_withdraw_eligibility(int(wd["user_id"]), conn=conn)
        wd_user = get_user(int(wd["user_id"]))
        username = f"@{wd_user['username']}" if wd_user and wd_user.get("username") else "No username"
        updated_text = (
            "📤 New Withdrawal Request!\n\n"
            f"👤 User: {username}\n"
            f"🆔 UserID: {wd['user_id']}\n"
            f"💰 Amount: {pretty_amount(wd['amount_stars'])} ⭐\n"
            f"🕒 Time: {now_pretty(wd.get('created_at'))}\n"
            "❌ Status: Rejected"
        )
        try:
            if wd.get("channel_message_id"):
                await context.bot.edit_message_text(chat_id=PAYMENT_CHANNEL, message_id=wd["channel_message_id"], text=updated_text)
        except Exception:
            pass
        try:
            if wd.get("admin_message_id"):
                await context.bot.edit_message_text(chat_id=ADMIN_ID, message_id=wd["admin_message_id"], text=updated_text)
        except Exception:
            pass
        try:
            await context.bot.send_message(int(wd["user_id"]), f"❌ Your withdrawal has been rejected: {pretty_amount(wd['amount_stars'])} ⭐", reply_markup=main_menu(int(wd["user_id"])))
        except Exception:
            pass
        await query.message.reply_text("❌ Withdrawal rejected.")
        return

    if data.startswith("proof_ok_"):
        if user.id != ADMIN_ID:
            return
        record_id = int(data.split("_")[-1])
        row = fetch_one(
            """
            SELECT ut.*, t.reward_stars, t.channel_title
            FROM user_tasks ut
            JOIN tasks t ON t.id = ut.task_id
            WHERE ut.id = %s
            """,
            (record_id,),
        )
        if not row or row.get("status") != "pending_review":
            return
        ok, reason = complete_exact_task_reward(int(row["user_id"]), int(row["task_id"]), decimalize(row["reward_stars"]))
        if not ok:
            await query.message.reply_text(f"Could not approve proof: {reason}")
            return
        execute(
            "UPDATE user_tasks SET admin_review_message_id = %s WHERE id = %s",
            (query.message.message_id, record_id),
        )
        try:
            await context.bot.send_message(int(row["user_id"]), t(int(row["user_id"]), "proof_approved", stars=pretty_amount(row["reward_stars"])), reply_markup=main_menu(int(row["user_id"])))
        except Exception:
            pass
        await query.message.reply_text(f"✅ Proof approved for user {row['user_id']} / task {row['task_id']}")
        return

    if data.startswith("proof_no_"):
        if user.id != ADMIN_ID:
            return
        record_id = int(data.split("_")[-1])
        row = fetch_one("SELECT * FROM user_tasks WHERE id = %s", (record_id,))
        if not row or row.get("status") != "pending_review":
            return
        execute(
            """
            UPDATE user_tasks
            SET status = 'rejected',
                reward_removed = 1,
                rejection_reason = %s,
                last_checked_at = %s,
                admin_review_message_id = %s
            WHERE id = %s
            """,
            ("Rejected by admin", now_iso(), query.message.message_id, record_id),
        )
        try:
            await context.bot.send_message(int(row["user_id"]), t(int(row["user_id"]), "proof_rejected"), reply_markup=main_menu(int(row["user_id"])))
        except Exception:
            pass
        await query.message.reply_text(f"❌ Proof rejected for user {row['user_id']} / task {row['task_id']}")
        return

    if data == "admin_add_kind_channel":
        if user.id != ADMIN_ID:
            return
        context.user_data["new_task_type"] = "channel"
        context.user_data["task_type"] = "channel"
        context.user_data["admin_flow"] = "addtask_link"
        await query.message.reply_text(t(user.id, "addtask_link"), reply_markup=cancel_reply_keyboard())
        return

    if data == "admin_add_kind_reaction":
        if user.id != ADMIN_ID:
            return
        context.user_data["new_task_type"] = "reaction"
        context.user_data["task_type"] = "reaction"
        context.user_data["admin_flow"] = "addtask_post_link"
        await query.message.reply_text(t(user.id, "addtask_post_link"), reply_markup=cancel_reply_keyboard())
        return

    if data == "admin_add_kind_botlink":
        if user.id != ADMIN_ID:
            return
        context.user_data["new_task_type"] = "bot_link"
        context.user_data["task_type"] = "bot_link"
        context.user_data["admin_flow"] = "addtask_bot_link"
        await query.message.reply_text(t(user.id, "addtask_bot_link"), reply_markup=cancel_reply_keyboard())
        return


    if data == "admin_add_kind_youtube":
        if user.id != ADMIN_ID:
            return
        context.user_data["new_task_type"] = "youtube"
        context.user_data["task_type"] = "youtube"
        context.user_data["admin_flow"] = "addtask_post_link"
        await query.message.reply_text(t(user.id, "addtask_post_link"), reply_markup=cancel_reply_keyboard())
        return

    if data == "admin_add_kind_facebook":
        if user.id != ADMIN_ID:
            return
        context.user_data["new_task_type"] = "facebook"
        context.user_data["task_type"] = "facebook"
        context.user_data["admin_flow"] = "addtask_post_link"
        await query.message.reply_text(t(user.id, "addtask_post_link"), reply_markup=cancel_reply_keyboard())
        return


# =====================================
# USER ROUTER
# =====================================
async def user_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if not await guard_user_access(update, context):
        return

    user = update.effective_user
    await process_leave_penalties_for_user(context.bot, user.id)
    text = (update.message.text or "").strip()

    if text == "📊 Statistics":
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
                users=int((total_users or {}).get("c") or 0),
                today=int((today_users or {}).get("c") or 0),
                stars=pretty_amount((total_stars or {}).get("s") or 0),
                admin_stars=pretty_amount(get_stars(ADMIN_ID)),
                tasks=int((active_tasks or {}).get("c") or 0),
            ),
            reply_markup=main_menu(user.id),
        )
        return

    if text == "📣 Broadcast":
        if user.id != ADMIN_ID:
            await update.message.reply_text(t(user.id, "admin_only"), reply_markup=main_menu(user.id))
            return
        context.user_data["admin_flow"] = "broadcast"
        await update.message.reply_text(t(user.id, "broadcast_prompt"), reply_markup=cancel_reply_keyboard())
        return

    if text == "🛠 Add Task":
        if user.id != ADMIN_ID:
            await update.message.reply_text(t(user.id, "admin_only"), reply_markup=main_menu(user.id))
            return
        context.user_data["admin_flow"] = None
        await update.message.reply_text(t(user.id, "addtask_kind"), reply_markup=add_task_kind_keyboard(user.id))
        return

    if text == "🗑 Remove Task":
        if user.id != ADMIN_ID:
            await update.message.reply_text(t(user.id, "admin_only"), reply_markup=main_menu(user.id))
            return
        tasks = fetch_all("SELECT id, channel_title, task_type FROM tasks WHERE status = 'active' ORDER BY id DESC")
        if not tasks:
            await update.message.reply_text("No active tasks.", reply_markup=main_menu(user.id))
            return
        buttons_list = [[InlineKeyboardButton(f"#{task['id']} - {task['channel_title']} ({task['task_type']})", callback_data=f"remove_task_{task['id']}")] for task in tasks]
        buttons_list.append([InlineKeyboardButton("⬅️ Back", callback_data="back_main")])
        await update.message.reply_text(t(user.id, "removetask_prompt"), reply_markup=InlineKeyboardMarkup(buttons_list))
        return

    if text == "➕ Add Balance":
        if user.id != ADMIN_ID:
            await update.message.reply_text(t(user.id, "admin_only"), reply_markup=main_menu(user.id))
            return
        context.user_data["admin_flow"] = "addbalance"
        await update.message.reply_text(t(user.id, "addbalance_prompt"), reply_markup=cancel_reply_keyboard())
        return

    if text == "🌐 Language":
        await update.message.reply_text(t(user.id, "choose_lang"), reply_markup=lang_keyboard())
        return

    if text == "⭐ My Stars":
        await update.message.reply_text(t(user.id, "my_stars", stars=pretty_amount(get_stars(user.id))), reply_markup=main_menu(user.id))
        return

    if text == "👥 Referral":
        await update.message.reply_text(t(user.id, "referral", link=referral_link(user.id), count=referral_count(user.id)), reply_markup=main_menu(user.id))
        return

    if text == "📝 Tasks":
        await send_task_list(update.message, context.bot, user.id, page=0)
        return

    if text == "🎁 Bonus":
        row = get_user(user.id)
        last_bonus = parse_dt((row or {}).get("last_bonus_at"))
        if last_bonus:
            next_dt = last_bonus + timedelta(hours=BONUS_INTERVAL_HOURS)
            if now_utc() < next_dt:
                await update.message.reply_text(
                    t(user.id, "bonus_wait", remaining=human_remaining(next_dt - now_utc())),
                    reply_markup=main_menu(user.id),
                )
                return
        add_stars(user.id, DAILY_BONUS_STARS)
        execute("UPDATE users SET last_bonus_at = %s WHERE user_id = %s", (now_iso(), user.id))
        await update.message.reply_text(t(user.id, "bonus_added", stars=pretty_amount(DAILY_BONUS_STARS)), reply_markup=main_menu(user.id))
        return

    if text == "🏧 Withdraw":
        await process_leave_penalties_for_user(context.bot, user.id)
        await update.message.reply_text(t(user.id, "withdraw_choose"), reply_markup=withdraw_keyboard(user.id))
        return

    if text == "ℹ️ About Us":
        await update.message.reply_text(t(user.id, "about"), reply_markup=main_menu(user.id))
        return

    if text == "📞 Support":
        await update.message.reply_text(t(user.id, "support", username=SUPPORT_USERNAME), reply_markup=main_menu(user.id))
        return

    await update.message.reply_text(t(user.id, "intro"), reply_markup=main_menu(user.id))


# =====================================
# MEDIA / PROOF FLOW
# =====================================
async def proof_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if not await guard_user_access(update, context):
        return

    user = update.effective_user
    task_id = context.user_data.get("awaiting_proof_task_id")
    if not task_id:
        await update.message.reply_text(t(user.id, "intro"), reply_markup=main_menu(user.id))
        return

    task = get_task(int(task_id))
    if not task or task.get("status") != "active":
        context.user_data.pop("awaiting_proof_task_id", None)
        context.user_data.pop("awaiting_proof_page", None)
        await update.message.reply_text("Task not found.", reply_markup=main_menu(user.id))
        return

    completion = get_task_completion(user.id, int(task_id))
    if completion and completion.get("status") == "completed" and int(completion.get("reward_removed") or 0) == 0:
        context.user_data.pop("awaiting_proof_task_id", None)
        context.user_data.pop("awaiting_proof_page", None)
        await update.message.reply_text(t(user.id, "task_already"), reply_markup=main_menu(user.id))
        return

    photo = update.message.photo[-1]
    mark_proof_pending(user.id, int(task_id), photo.file_id, photo.file_unique_id, update.message.message_id)

    caption = (
        f"📸 Proof submitted\n"
        f"👤 User: {user.id} / @{user.username or 'no_username'}\n"
        f"📝 Task: {task['channel_title']}\n"
        f"🆔 Task ID: {task['id']}\n"
        f"🕒 {now_pretty()}"
    )
    sent = None
    try:
        sent = await context.bot.send_photo(
            chat_id=ADMIN_ID,
            photo=photo.file_id,
            caption=caption,
            reply_markup=proof_review_keyboard(get_task_completion(user.id, int(task_id))["id"]),
        )
    except Exception:
        pass

    if sent:
        execute("UPDATE user_tasks SET admin_review_message_id = %s WHERE user_id = %s AND task_id = %s", (sent.message_id, user.id, int(task_id)))

    context.user_data.pop("awaiting_proof_task_id", None)
    context.user_data.pop("awaiting_proof_page", None)
    await update.message.reply_text(t(user.id, "proof_saved"), reply_markup=main_menu(user.id))


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
    rows = fetch_all("SELECT user_id, username, stars, referrer_id, is_banned FROM users ORDER BY created_at DESC LIMIT 100")
    text = "\n".join(
        [
            f"{r['user_id']} | @{r['username'] or 'no_username'} | ⭐ {pretty_amount(r['stars'])} | ref_by: {r.get('referrer_id') or '-'} | banned: {bool(r.get('is_banned'))}"
            for r in rows
        ]
    ) or "No users"
    await update.message.reply_text(text)


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
    lines = ["🏆 Referral Stats\n"]
    for i, row in enumerate(rows, start=1):
        referrer_id = int(row["referrer_id"])
        refs = get_user_refs(referrer_id)
        user = get_user(referrer_id)
        username = f"@{user['username']}" if user and user.get("username") else str(referrer_id)
        invited = []
        for item in refs:
            if item.get("username"):
                invited.append(f"@{item['username']} ({item['id']})")
            else:
                invited.append(str(item["id"]))
        lines.append(f"{i}. {username} - {int(row['refs'])} invited\n👉 {', '.join(invited) if invited else 'No users'}\n")
    await update.message.reply_text("\n".join(lines))


async def admin_withdraws(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return
    rows = fetch_all("SELECT * FROM withdrawals WHERE status = 'pending' ORDER BY created_at DESC LIMIT 50")
    text = "\n".join([f"#{r['id']} | User {r['user_id']} | ⭐ {pretty_amount(r['amount_stars'])} | {r['status']}" for r in rows]) or "No pending withdraws"
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
    pending_proofs = fetch_one("SELECT COUNT(*) AS c FROM user_tasks WHERE status = 'pending_review'")
    await update.message.reply_text(
        "📊 Bot Stats\n\n"
        f"Users: {int((total_users or {}).get('c') or 0)}\n"
        f"Active Tasks: {int((total_tasks or {}).get('c') or 0)}\n"
        f"Withdraw Requests: {int((total_withdraws or {}).get('c') or 0)}\n"
        f"Pending Proofs: {int((pending_proofs or {}).get('c') or 0)}\n"
        f"Total User Stars: {pretty_amount((total_stars or {}).get('s') or 0)}\n"
        f"Admin Stars: {pretty_amount(get_stars(ADMIN_ID))}"
    )


async def admin_taskslist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return
    rows = task_stats_rows(100)
    if not rows:
        await update.message.reply_text("No tasks")
        return
    lines = []
    for row in rows:
        created = parse_dt(row.get("created_at"))
        duration = human_remaining(now_utc() - created) if created else "0h 0m"
        lines.append(
            f"#{row['id']} | {row['channel_title']} | {row['task_type']} | ⭐ {pretty_amount(row['reward_stars'])} | joins: {int(row['join_count'])} | active: {duration}"
        )
    await update.message.reply_text("\n".join(lines))


async def admin_taskstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return
    rows = task_stats_rows(100)
    if not rows:
        await update.message.reply_text("No active task stats")
        return
    lines = ["📊 Active Task Stats\n"]
    for row in rows:
        created = parse_dt(row.get("created_at"))
        duration = human_remaining(now_utc() - created) if created else "0h 0m"
        lines.append(
            f"#{row['id']} | {row['channel_title']}\n"
            f"🧩 type: {row['task_type']}\n"
            f"⭐ reward: {pretty_amount(row['reward_stars'])}\n"
            f"👥 join count: {int(row['join_count'])}\n"
            f"⏱ active: {duration}\n"
        )
    await update.message.reply_text("\n".join(lines))


async def admin_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return
    if not context.args:
        await update.message.reply_text("Usage: /ban USER_ID [reason]")
        return
    try:
        target_id = int(context.args[0])
    except Exception:
        await update.message.reply_text("Invalid USER_ID")
        return
    reason = " ".join(context.args[1:]).strip() or "Fake activity"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET is_banned = TRUE, banned_at = %s, ban_reason = %s, stars = 0 WHERE user_id = %s",
                (now_iso(), reason, target_id),
            )
            cur.execute(
                "UPDATE withdrawals SET status = 'rejected', rejected_at = %s, reason = %s WHERE user_id = %s AND status = 'pending'",
                (now_iso(), f"Auto-rejected on ban: {reason}", target_id),
            )
            update_withdraw_eligibility(target_id, conn=conn)
    try:
        await context.bot.send_message(target_id, t(target_id, "banned"))
    except Exception:
        pass
    await update.message.reply_text(f"✅ User {target_id} banned. Balance zeroed and pending withdraws rejected.")


async def admin_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return
    if not context.args:
        await update.message.reply_text("Usage: /unban USER_ID")
        return
    try:
        target_id = int(context.args[0])
    except Exception:
        await update.message.reply_text("Invalid USER_ID")
        return
    execute("UPDATE users SET is_banned = FALSE, ban_reason = NULL WHERE user_id = %s", (target_id,))
    await update.message.reply_text(f"✅ User {target_id} unbanned.")


async def admin_flow_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not is_private(update) or update.effective_user.id != ADMIN_ID:
        return False
    flow = context.user_data.get("admin_flow")
    if not flow:
        return False
    text = (update.message.text or "").strip()

    if text.lower() in ("cancel", "/cancel", "❌ cancel", "back", "⬅️ back"):
        for key in [
            "admin_flow",
            "new_task_type",
            "task_chat_username",
            "task_link",
            "task_title",
            "task_post_link",
            "task_bot_link",
        ]:
            context.user_data.pop(key, None)
        await update.message.reply_text(t(update.effective_user.id, "cancelled"), reply_markup=main_menu(update.effective_user.id))
        return True

    if flow == "broadcast":
        users = fetch_all("SELECT user_id FROM users WHERE COALESCE(is_banned, FALSE) = FALSE")
        sent = 0
        failed = 0
        for row in users:
            try:
                await context.bot.send_message(chat_id=row["user_id"], text=text)
                sent += 1
            except Exception:
                failed += 1
        context.user_data.pop("admin_flow", None)
        await update.message.reply_text(f"✅ Sent: {sent}\n❌ Failed: {failed}", reply_markup=main_menu(update.effective_user.id))
        return True

    if flow == "addtask_link":
        username = extract_chat_username(text)
        if not username:
            await update.message.reply_text("Invalid link or @username", reply_markup=cancel_reply_keyboard())
            return True
        context.user_data["task_chat_username"] = username
        context.user_data["task_link"] = task_url(text)
        context.user_data["admin_flow"] = "addtask_title"
        await update.message.reply_text(t(update.effective_user.id, "addtask_title"), reply_markup=cancel_reply_keyboard())
        return True

    if flow == "addtask_post_link":
        if not text.startswith("http"):
            await update.message.reply_text("Send a valid public post link.", reply_markup=cancel_reply_keyboard())
            return True
        context.user_data["task_post_link"] = text
        context.user_data["task_link"] = text
        context.user_data["admin_flow"] = "addtask_title"
        await update.message.reply_text(t(update.effective_user.id, "addtask_title"), reply_markup=cancel_reply_keyboard())
        return True

    if flow == "addtask_bot_link":
        if not text.startswith("http"):
            await update.message.reply_text("Send a valid bot link.", reply_markup=cancel_reply_keyboard())
            return True
        context.user_data["task_bot_link"] = text
        context.user_data["task_link"] = text
        context.user_data["admin_flow"] = "addtask_title"
        await update.message.reply_text(t(update.effective_user.id, "addtask_title"), reply_markup=cancel_reply_keyboard())
        return True

    if flow == "addtask_title":
        context.user_data["task_title"] = text
        context.user_data["admin_flow"] = "addtask_reward"
        await update.message.reply_text(t(update.effective_user.id, "addtask_reward"), reply_markup=cancel_reply_keyboard())
        return True

    if flow == "addtask_reward":
        try:
            reward = decimalize(text)
            if reward <= 0:
                raise ValueError
        except Exception:
            await update.message.reply_text("Invalid reward. Example: 0.5", reply_markup=cancel_reply_keyboard())
            return True

        task_type = context.user_data.get("task_type") or context.user_data.get("new_task_type") or "channel"
        if task_type == "bot":
            task_type = "bot_link"
        title = context.user_data.get("task_title") or "Task"
        created_id = add_task_record(
            task_type=task_type,
            channel_title=title,
            link=context.user_data.get("task_link"),
            reward_stars=reward,
            chat_username=context.user_data.get("task_chat_username"),
            requires_proof=(task_type in ("reaction", "bot_link", "facebook", "youtube")),
            post_link=context.user_data.get("task_post_link"),
            bot_link=context.user_data.get("task_bot_link"),
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

        await update.message.reply_text(f"✅ Task added #{created_id}", reply_markup=main_menu(update.effective_user.id))
        notify_text = get_text("ps", "new_task", title=title, reward=pretty_amount(reward))
        await notify_all_users(context, notify_text)
        return True

    if flow == "addbalance":
        try:
            amount = decimalize(text)
            if amount <= 0:
                raise ValueError
        except Exception:
            await update.message.reply_text("Invalid amount. Example: 1000", reply_markup=cancel_reply_keyboard())
            return True
        add_stars(ADMIN_ID, amount)
        context.user_data.pop("admin_flow", None)
        await update.message.reply_text(
            t(update.effective_user.id, "addbalance_done", amount=pretty_amount(amount), new_balance=pretty_amount(get_stars(ADMIN_ID))),
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
    await query.answer()
    user = update.effective_user
    data = query.data or ""
    if not data.startswith("remove_task_"):
        return
    if user.id != ADMIN_ID:
        await query.answer("Admin only", show_alert=True)
        return
    task_id = int(data.split("_")[-1])
    set_task_removed(task_id)
    await query.message.reply_text(f"✅ Task #{task_id} removed", reply_markup=main_menu(user.id))


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
    app.add_handler(CommandHandler("ban", admin_ban))
    app.add_handler(CommandHandler("unban", admin_unban))

    app.add_handler(CallbackQueryHandler(remove_task_callback, pattern=r"^remove_task_\d+$"))
    app.add_handler(CallbackQueryHandler(buttons))
    app.add_handler(ChatMemberHandler(track_bot_chats, ChatMemberHandler.MY_CHAT_MEMBER))

    async def combined_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
        handled = await admin_flow_router(update, context)
        if handled:
            return
        await user_router(update, context)

    app.add_handler(MessageHandler(filters.PHOTO & ~filters.COMMAND, proof_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, combined_router))

    if app.job_queue:
        app.job_queue.run_repeating(periodic_leave_check, interval=LEAVE_CHECK_INTERVAL_HOURS * 3600, first=600)
        app.job_queue.run_repeating(daily_promo_post, interval=PROMO_INTERVAL_HOURS * 3600, first=900)

    logger.info("EasyEarn bot is running...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
