import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL", "")
BOT_USERNAME = os.getenv("BOT_USERNAME", "EasyEarnAppBot")

FORCE_JOIN_CHANNELS = [
    ("@easyearnofficial1222", "https://t.me/easyearnofficial1222"),
    ("@easyearnpayments", "https://t.me/easyearnpayments"),
]

PAYMENT_CHANNEL = "@easyearnpayments"
PROMO_MESSAGE = (
    "📢 Khan Digital Group\n"
    "https://t.me/haqyarserviceso1\n\n"
    "📢 Khan Technical\n"
    "https://t.me/Solutions3232\n\n"
    "👤 @haqiarkhan12"
)

HESAB_PAY = "+93708310201"
ATOMA_PAY = "+93770876916"
BINANCE_UID = "1182541650"
USDT_RATE_AFN = 60
MIN_USDT_DEPOSIT = 2
MIN_TASK_ADD_BALANCE = 50
TASK_REWARD_AFN = 1
MIN_WITHDRAW_AFN = 30
MIN_DEPOSIT_AFN = 100
DEPOSIT_FEE_PERCENT = 5
REFERRAL_REWARD_AFN = 10
DAILY_BONUS_AFN = 1
AUTO_LEAVE_PENALTY_CHECK_HOURS = 24

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID is missing")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is missing")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# =========================
# DB
# =========================
def db_connect():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


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


def execute(query: str, params: tuple = (), returning: bool = False):
    conn = db_connect()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query, params)
    result = cur.fetchone() if returning else None
    conn.commit()
    cur.close()
    conn.close()
    return dict(result) if result else None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db() -> None:
    conn = db_connect()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            lang TEXT DEFAULT 'ps',
            role TEXT,
            balance INTEGER DEFAULT 0,
            debt INTEGER DEFAULT 0,
            referrer_id BIGINT,
            referral_paid INTEGER DEFAULT 0,
            last_bonus_at TEXT,
            created_at TEXT
        )
        """
    )
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS balance INTEGER DEFAULT 0")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS debt INTEGER DEFAULT 0")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referrer_id BIGINT")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_paid INTEGER DEFAULT 0")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_bonus_at TEXT")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS lang TEXT DEFAULT 'ps'")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS role TEXT")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TEXT")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS full_name TEXT")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS campaigns (
            id SERIAL PRIMARY KEY,
            owner_user_id BIGINT NOT NULL,
            title_ps TEXT NOT NULL,
            title_en TEXT NOT NULL,
            reward_afn INTEGER NOT NULL DEFAULT 1,
            target_type TEXT NOT NULL,
            link TEXT NOT NULL,
            channel_title TEXT NOT NULL,
            chat_username TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            completed_count INTEGER DEFAULT 0,
            created_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_campaigns (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            campaign_id INTEGER NOT NULL,
            reward_given INTEGER DEFAULT 1,
            penalty_applied INTEGER DEFAULT 0,
            owner_user_id BIGINT,
            reward_afn INTEGER DEFAULT 1,
            status TEXT DEFAULT 'completed',
            created_at TEXT,
            last_checked_at TEXT,
            UNIQUE(user_id, campaign_id)
        )
        """
    )
    cur.execute("ALTER TABLE user_campaigns ADD COLUMN IF NOT EXISTS reward_given INTEGER DEFAULT 1")
    cur.execute("ALTER TABLE user_campaigns ADD COLUMN IF NOT EXISTS penalty_applied INTEGER DEFAULT 0")
    cur.execute("ALTER TABLE user_campaigns ADD COLUMN IF NOT EXISTS owner_user_id BIGINT")
    cur.execute("ALTER TABLE user_campaigns ADD COLUMN IF NOT EXISTS reward_afn INTEGER DEFAULT 1")
    cur.execute("ALTER TABLE user_campaigns ADD COLUMN IF NOT EXISTS last_checked_at TEXT")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS deposits (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            amount INTEGER NOT NULL,
            method TEXT NOT NULL,
            proof_file_id TEXT,
            usdt_amount NUMERIC(12,2),
            status TEXT DEFAULT 'pending',
            channel_message_id BIGINT,
            created_at TEXT
        )
        """
    )
    cur.execute("ALTER TABLE deposits ADD COLUMN IF NOT EXISTS usdt_amount NUMERIC(12,2)")
    cur.execute("ALTER TABLE deposits ADD COLUMN IF NOT EXISTS channel_message_id BIGINT")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS withdrawals (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            amount INTEGER NOT NULL,
            network TEXT NOT NULL,
            phone TEXT NOT NULL,
            full_name TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            channel_message_id BIGINT,
            created_at TEXT
        )
        """
    )
    cur.execute("ALTER TABLE withdrawals ADD COLUMN IF NOT EXISTS channel_message_id BIGINT")

    conn.commit()
    cur.close()
    conn.close()


# =========================
# HELPERS
# =========================
def ensure_user(user_id: int, username: str | None, full_name: str | None) -> None:
    user_id = int(user_id)
    row = fetch_one("SELECT * FROM users WHERE user_id = %s", (user_id,))
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


def ensure_admin_account() -> None:
    row = fetch_one("SELECT * FROM users WHERE user_id = %s", (ADMIN_ID,))
    if not row:
        execute(
            """
            INSERT INTO users (user_id, username, full_name, lang, role, balance, created_at)
            VALUES (%s, %s, %s, 'ps', 'client', 0, %s)
            """,
            (ADMIN_ID, "admin", "Admin", now_iso()),
        )


def get_user(user_id: int) -> Optional[dict]:
    return fetch_one("SELECT * FROM users WHERE user_id = %s", (user_id,))


def set_lang(user_id: int, lang: str) -> None:
    execute("UPDATE users SET lang = %s WHERE user_id = %s", (lang, user_id))


def user_lang(user_id: int) -> str:
    row = get_user(user_id)
    if not row:
        return "ps"
    lang = row.get("lang") or "ps"
    return lang if lang in ("ps", "en") else "ps"


def set_role(user_id: int, role: str) -> None:
    execute("UPDATE users SET role = %s WHERE user_id = %s", (role, user_id))


def get_balance(user_id: int) -> int:
    row = get_user(user_id)
    return int(row.get("balance", 0)) if row else 0


def get_debt(user_id: int) -> int:
    row = get_user(user_id)
    return int(row.get("debt", 0)) if row else 0


def set_balance(user_id: int, amount: int) -> None:
    execute("UPDATE users SET balance = %s WHERE user_id = %s", (amount, user_id))


def set_debt(user_id: int, amount: int) -> None:
    execute("UPDATE users SET debt = %s WHERE user_id = %s", (amount, user_id))


def add_reward_with_debt_clear(user_id: int, reward: int) -> tuple[int, int]:
    debt = get_debt(user_id)
    cleared = 0
    added = 0
    if debt > 0:
        if reward >= debt:
            cleared = debt
            reward -= debt
            set_debt(user_id, 0)
        else:
            cleared = reward
            set_debt(user_id, debt - reward)
            reward = 0
    if reward > 0:
        add_balance(user_id, reward)
        added = reward
    return cleared, added


def add_balance(user_id: int, amount: int) -> None:
    execute(
        "UPDATE users SET balance = COALESCE(balance, 0) + %s WHERE user_id = %s",
        (amount, user_id),
    )


def referral_link(user_id: int) -> str:
    return f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"


def referral_count(user_id: int) -> int:
    row = fetch_one("SELECT COUNT(*) AS c FROM users WHERE referrer_id = %s", (user_id,))
    return int(row["c"]) if row else 0


def deposit_fee(amount: int) -> tuple[int, int]:
    fee = int(round(amount * DEPOSIT_FEE_PERCENT / 100))
    return amount - fee, fee


def withdraw_fee(amount: int) -> tuple[int, int]:
    final_amount = amount
    return final_amount, 0


def extract_chat_username(link: str) -> Optional[str]:
    link = link.strip()
    m = re.search(r"(?:https?://)?t\.me/([A-Za-z0-9_]{4,})/?$", link)
    if m:
        return "@" + m.group(1)
    m = re.search(r"@([A-Za-z0-9_]{4,})$", link)
    if m:
        return "@" + m.group(1)
    return None


def get_campaign(campaign_id: int) -> Optional[dict]:
    return fetch_one("SELECT * FROM campaigns WHERE id = %s", (campaign_id,))


# =========================
# TEXTS
# =========================
TEXTS = {
    "ps": {
        "choose_lang": "ژبه انتخاب کړئ:",
        "choose_area": "مهرباني وکړئ برخه انتخاب کړئ:",
        "worker": "👷 ورکر",
        "client": "📢 کلاینت",
        "welcome": "ښه راغلاست EasyEarn Bot ته",
        "main_menu": "اصلي مینو",
        "force_join": "مهرباني وکړئ دواړه چینلونه جوین کړئ:",
        "joined_btn": "✅ جوین مې کړل",
        "join_failed": "اول دواړه چینلونه جوین کړئ.",
        "balance": "💰 ستاسو بیلانس: {balance} AFN\n📉 قرض: {debt} AFN",
        "bonus_added": "🎁 ورځنی بونس اضافه شو.",
        "bonus_wait": "⏳ تاسو نن بونس اخیستی.",
        "referral": "👥 ستاسو ریفرل لینک:\n{link}\n\nټول ریفرلونه: {count}",
        "tasks_empty": "فعلاً ټاسک وجود نه لري.",
        "task_done": "✅ ټاسک بشپړ شو.\nقرض خلاص: {cleared} AFN\nاضافه شو: {added} AFN",
        "task_already": "تاسو دا ټاسک مخکې بشپړ کړی.",
        "task_fail": "اول چینل جوین کړئ، بیا وریفای وکړئ.",
        "task_owner_low": "د ټاسک مالک کافي پیسې نه لري، ټاسک بند شو.",
        "withdraw_min": "د ویډرا لږ تر لږه اندازه 30 AFN ده.",
        "withdraw_low_balance": "ستاسو بیلانس کم دی.",
        "withdraw_ask_amount": "د ویډرا اندازه ولیکئ:",
        "withdraw_ask_name": "خپل بشپړ نوم ولیکئ:",
        "withdraw_ask_phone": "خپله شمېره ولیکئ:",
        "withdraw_ask_network": "شبکه ولیکئ: Afghan Wireless / Etisalat / Roshan / Salaam / Atoma",
        "withdraw_sent": "ستاسو د ویډرا غوښتنه ثبت شوه.",
        "deposit_menu": "ډیپازټ میتود انتخاب کړئ:",
        "deposit_hesab_info": "دې نمبر ته پیسې ولیږئ بیا screenshot راولېږئ:\n{number}\n\nکم حد: 100 AFN\nفیس: 5%",
        "deposit_atoma_info": "دې نمبر ته پیسې ولیږئ بیا screenshot راولېږئ:\n{number}\n\nکم حد: 100 AFN\nفیس: 5%",
        "deposit_binance_info": "Binance UID:\n{uid}\n\nکم حد: 2 USDT\n1 USDT = 60 AFN\nUSDT له لیږلو وروسته amount ولیکئ او بیا screenshot راولېږئ.",
        "deposit_amount_prompt": "هغه اندازه ولیکئ چې لیږلې مو ده:",
        "deposit_proof_prompt": "اوس screenshot راولېږئ.",
        "deposit_sent": "ستاسو د ډیپازټ غوښتنه ثبت شوه.",
        "campaigns_empty": "تاسو لا کمپاین نه لرئ.",
        "task_guide": "بوټ په خپل چینل/ګروپ کې اډمین کړئ، بیا public link راولېږئ.",
        "ask_title": "د چینل/ګروپ نوم راولېږئ:",
        "invalid_link": "public link یا username سم نه دی.",
        "campaign_created": "ټاسک جوړ شو.",
        "need_min_task_balance": "ته کافي پیسې نه لرې. اول بیلانس زیات کړه، بیا تاسک اډ کړه.",
        "approved_deposit": "ستاسو ډیپازټ تایید شو.",
        "approved_withdraw": "ستاسو ویډرا تایید شوه.",
        "rejected": "ستاسو غوښتنه رد شوه.",
        "new_task_broadcast": "📢 نوی ټاسک اضافه شو!\n🎁 انعام: 1 AFN",
        "task_list_title": "📢 موجود ټاسکونه",
        "choose_task_type": "ټاسک ډول انتخاب کړئ:",
        "task_type_channel": "📢 چینل ټاسک",
        "task_type_group": "👥 ګروپ ټاسک",
    },
    "en": {
        "choose_lang": "Choose language:",
        "choose_area": "Please choose a section:",
        "worker": "👷 Worker",
        "client": "📢 Client",
        "welcome": "Welcome to EasyEarn Bot",
        "main_menu": "Main Menu",
        "force_join": "Please join both channels first:",
        "joined_btn": "✅ I Joined",
        "join_failed": "Please join both channels first.",
        "balance": "💰 Your balance: {balance} AFN\n📉 Debt: {debt} AFN",
        "bonus_added": "🎁 Daily bonus added.",
        "bonus_wait": "⏳ You already claimed bonus today.",
        "referral": "👥 Your referral link:\n{link}\n\nTotal referrals: {count}",
        "tasks_empty": "No tasks available right now.",
        "task_done": "✅ Task completed.\nDebt cleared: {cleared} AFN\nAdded: {added} AFN",
        "task_already": "You already completed this task.",
        "task_fail": "Join the channel first, then verify.",
        "task_owner_low": "Task owner balance is low, task was stopped.",
        "withdraw_min": "Minimum withdraw is 30 AFN.",
        "withdraw_low_balance": "Your balance is too low.",
        "withdraw_ask_amount": "Enter withdraw amount:",
        "withdraw_ask_name": "Enter your full name:",
        "withdraw_ask_phone": "Enter your phone number:",
        "withdraw_ask_network": "Enter network: Afghan Wireless / Etisalat / Roshan / Salaam / Atoma",
        "withdraw_sent": "Your withdraw request was submitted.",
        "deposit_menu": "Choose deposit method:",
        "deposit_hesab_info": "Send money to this number then send screenshot:\n{number}\n\nMinimum: 100 AFN\nFee: 5%",
        "deposit_atoma_info": "Send money to this number then send screenshot:\n{number}\n\nMinimum: 100 AFN\nFee: 5%",
        "deposit_binance_info": "Binance UID:\n{uid}\n\nMinimum: 2 USDT\n1 USDT = 60 AFN\nAfter sending USDT, enter amount then send screenshot.",
        "deposit_amount_prompt": "Enter amount you sent:",
        "deposit_proof_prompt": "Now send screenshot.",
        "deposit_sent": "Your deposit request was submitted.",
        "campaigns_empty": "You have no campaigns yet.",
        "task_guide": "Add bot as admin in your channel/group, then send public link.",
        "ask_title": "Send channel/group title:",
        "invalid_link": "Public link or username is invalid.",
        "campaign_created": "Task created.",
        "need_min_task_balance": "You do not have enough balance. Increase balance first, then add task.",
        "approved_deposit": "Your deposit was approved.",
        "approved_withdraw": "Your withdraw was approved.",
        "rejected": "Your request was rejected.",
        "new_task_broadcast": "📢 New task added!\n🎁 Reward: 1 AFN",
        "task_list_title": "📢 Available Tasks",
        "choose_task_type": "Choose task type:",
        "task_type_channel": "📢 Channel Task",
        "task_type_group": "👥 Group Task",
    },
}


def t(user_id: int, key: str, **kwargs) -> str:
    lang = user_lang(user_id)
    return TEXTS[lang][key].format(**kwargs)


# =========================
# KEYBOARDS
# =========================
def lang_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🇦🇫 پښتو", callback_data="lang_ps")],
        [InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
    ])


def force_join_keyboard(user_id: int):
    rows = []
    for _, link in FORCE_JOIN_CHANNELS:
        rows.append([InlineKeyboardButton("📢 Join Channel", url=link)])
    rows.append([InlineKeyboardButton(t(user_id, "joined_btn"), callback_data="check_force_join")])
    return InlineKeyboardMarkup(rows)


def main_menu_keyboard(user_id: int):
    lang = user_lang(user_id)
    if lang == "ps":
        rows = [
            [InlineKeyboardButton("👷 ورکر", callback_data="open_worker_menu")],
            [InlineKeyboardButton("📢 کلاینت", callback_data="open_client_menu")],
            [InlineKeyboardButton("🌐 ژبه بدلول", callback_data="change_lang")],
        ]
    else:
        rows = [
            [InlineKeyboardButton("👷 Worker", callback_data="open_worker_menu")],
            [InlineKeyboardButton("📢 Client", callback_data="open_client_menu")],
            [InlineKeyboardButton("🌐 Change Language", callback_data="change_lang")],
        ]
    return InlineKeyboardMarkup(rows)


def worker_menu(user_id: int):
    lang = user_lang(user_id)
    if lang == "ps":
        rows = [
            [InlineKeyboardButton("💰 زما بیلانس", callback_data="worker_balance"), InlineKeyboardButton("📢 دندې", callback_data="worker_tasks")],
            [InlineKeyboardButton("🎁 ورځنی بونس", callback_data="worker_bonus"), InlineKeyboardButton("👥 ریفرل", callback_data="worker_referral")],
            [InlineKeyboardButton("💸 ویډرا", callback_data="worker_withdraw"), InlineKeyboardButton("⬅️ اصلي مینو", callback_data="back_main_menu")],
        ]
    else:
        rows = [
            [InlineKeyboardButton("💰 My Balance", callback_data="worker_balance"), InlineKeyboardButton("📢 Tasks", callback_data="worker_tasks")],
            [InlineKeyboardButton("🎁 Daily Bonus", callback_data="worker_bonus"), InlineKeyboardButton("👥 Referral", callback_data="worker_referral")],
            [InlineKeyboardButton("💸 Withdraw", callback_data="worker_withdraw"), InlineKeyboardButton("⬅️ Main Menu", callback_data="back_main_menu")],
        ]
    return InlineKeyboardMarkup(rows)


def client_menu(user_id: int):
    lang = user_lang(user_id)
    if lang == "ps":
        rows = [
            [InlineKeyboardButton("💰 زما بیلانس", callback_data="client_balance"), InlineKeyboardButton("💳 ډیپازټ", callback_data="client_deposit")],
            [InlineKeyboardButton("➕ اډ تاسک", callback_data="client_add_task"), InlineKeyboardButton("📊 زما کمپاینونه", callback_data="client_campaigns")],
            [InlineKeyboardButton("🎁 ورځنی بونس", callback_data="client_bonus"), InlineKeyboardButton("👥 ریفرل", callback_data="client_referral")],
            [InlineKeyboardButton("⬅️ اصلي مینو", callback_data="back_main_menu")],
        ]
    else:
        rows = [
            [InlineKeyboardButton("💰 My Balance", callback_data="client_balance"), InlineKeyboardButton("💳 Deposit", callback_data="client_deposit")],
            [InlineKeyboardButton("➕ Add Task", callback_data="client_add_task"), InlineKeyboardButton("📊 My Campaigns", callback_data="client_campaigns")],
            [InlineKeyboardButton("🎁 Daily Bonus", callback_data="client_bonus"), InlineKeyboardButton("👥 Referral", callback_data="client_referral")],
            [InlineKeyboardButton("⬅️ Main Menu", callback_data="back_main_menu")],
        ]
    return InlineKeyboardMarkup(rows)


def choose_task_type_keyboard(user_id: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(t(user_id, "task_type_channel"), callback_data="task_type_channel")],
        [InlineKeyboardButton(t(user_id, "task_type_group"), callback_data="task_type_group")],
        [InlineKeyboardButton("⬅️", callback_data="open_client_menu")],
    ])


def deposit_method_keyboard(user_id: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Hesab Pay", callback_data="deposit_method_hesab")],
        [InlineKeyboardButton("Atoma Pay", callback_data="deposit_method_atoma")],
        [InlineKeyboardButton("Binance", callback_data="deposit_method_binance")],
        [InlineKeyboardButton("⬅️", callback_data="open_client_menu")],
    ])


def campaign_list_keyboard(user_id: int, campaigns: list[dict]):
    rows = []
    for c in campaigns:
        title = c["title_ps"] if user_lang(user_id) == "ps" else c["title_en"]
        rows.append([InlineKeyboardButton(title, callback_data=f"open_campaign_{c['id']}")])
    rows.append([InlineKeyboardButton("⬅️", callback_data="open_worker_menu")])
    return InlineKeyboardMarkup(rows)


def task_card_keyboard(user_id: int, campaign_id: int, link: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Open", url=link)],
        [InlineKeyboardButton("✅ Verify", callback_data=f"verify_campaign_{campaign_id}")],
        [InlineKeyboardButton("⬅️", callback_data="worker_tasks")],
    ])


# =========================
# CHANNEL CHECK + PENALTY
# =========================
async def check_join(bot, chat_username: str, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=chat_username, user_id=user_id)
        return member.status in ("member", "administrator", "creator", "owner")
    except Exception as e:
        logger.info("Join check failed for %s -> %s: %s", chat_username, user_id, e)
        return False


async def check_force_join_all(bot, user_id: int) -> bool:
    for username, _ in FORCE_JOIN_CHANNELS:
        ok = await check_join(bot, username, user_id)
        if not ok:
            return False
    return True


async def process_user_leave_penalties(bot, user_id: int) -> tuple[int, int]:
    rows = fetch_all(
        """
        SELECT uc.id, uc.owner_user_id, uc.reward_afn, uc.campaign_id, c.chat_username
        FROM user_campaigns uc
        JOIN campaigns c ON uc.campaign_id = c.id
        WHERE uc.user_id = %s
          AND uc.status = 'completed'
          AND uc.penalty_applied = 0
          AND uc.reward_given = 1
        """,
        (user_id,),
    )

    total_penalty = 0
    total_count = 0

    for row in rows:
        still_joined = await check_join(bot, row["chat_username"], user_id)
        if still_joined:
            execute(
                "UPDATE user_campaigns SET last_checked_at = %s WHERE id = %s",
                (now_iso(), row["id"]),
            )
            continue

        reward = int(row.get("reward_afn", TASK_REWARD_AFN))
        current_balance = get_balance(user_id)
        if current_balance >= reward:
            add_balance(user_id, -reward)
        else:
            if current_balance > 0:
                add_balance(user_id, -current_balance)
                remaining = reward - current_balance
            else:
                remaining = reward
            set_debt(user_id, get_debt(user_id) + remaining)

        add_balance(row["owner_user_id"], reward)
        execute(
            """
            UPDATE user_campaigns
            SET penalty_applied = 1, status = 'left', last_checked_at = %s
            WHERE id = %s
            """,
            (now_iso(), row["id"]),
        )
        total_penalty += reward
        total_count += 1

    return total_penalty, total_count


async def periodic_leave_check(context: ContextTypes.DEFAULT_TYPE):
    users = fetch_all("SELECT user_id FROM users")
    for u in users:
        try:
            total_penalty, total_count = await process_user_leave_penalties(context.bot, u["user_id"])
            if total_count > 0:
                try:
                    await context.bot.send_message(
                        chat_id=u["user_id"],
                        text=f"⚠️ تاسو {total_count} چینلونه پرېښي وو، نو {total_penalty} AFN بېرته کم شول.",
                    )
                except Exception:
                    pass
        except Exception as e:
            logger.exception("periodic leave check failed: %s", e)


# =========================
# PROMO POST
# =========================
async def daily_promo_post(context: ContextTypes.DEFAULT_TYPE):
    for username, _ in FORCE_JOIN_CHANNELS:
        try:
            await context.bot.send_message(chat_id=username, text=PROMO_MESSAGE)
        except Exception:
            pass


# =========================
# MAIN MENU
# =========================
async def show_main_menu(chat_id: int, user_id: int, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=chat_id,
        text=t(user_id, "main_menu") + "\n\n" + t(user_id, "choose_area"),
        reply_markup=main_menu_keyboard(user_id),
    )


# =========================
# START
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(int(user.id), user.username or "", user.full_name or "")

    if context.args:
        arg = context.args[0]
        if arg.startswith("ref_"):
            try:
                referrer_id = int(arg.split("_", 1)[1])
                row = get_user(user.id)
                if row and not row.get("referrer_id") and referrer_id != user.id:
                    execute("UPDATE users SET referrer_id = %s WHERE user_id = %s", (referrer_id, user.id))
            except Exception:
                pass

    row = get_user(user.id)
    if not row or not row.get("lang"):
        await update.message.reply_text(TEXTS["ps"]["choose_lang"], reply_markup=lang_keyboard())
        return

    if not await check_force_join_all(context.bot, user.id):
        text = t(user.id, "force_join") + "\n\n"
        for username, _ in FORCE_JOIN_CHANNELS:
            text += f"{username}\n"
        await update.message.reply_text(text, reply_markup=force_join_keyboard(user.id))
        return

    await process_user_leave_penalties(context.bot, user.id)
    await show_main_menu(update.effective_chat.id, user.id, context)


# =========================
# CALLBACKS
# =========================
async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    ensure_user(user.id, user.username or "", user.full_name or "")
    data = query.data

    if data == "lang_ps":
        set_lang(user.id, "ps")
        if not await check_force_join_all(context.bot, user.id):
            text = t(user.id, "force_join") + "\n\n"
            for username, _ in FORCE_JOIN_CHANNELS:
                text += f"{username}\n"
            await query.edit_message_text(text, reply_markup=force_join_keyboard(user.id))
            return
        await query.edit_message_text(t(user.id, "choose_area"), reply_markup=main_menu_keyboard(user.id))
        return

    if data == "lang_en":
        set_lang(user.id, "en")
        if not await check_force_join_all(context.bot, user.id):
            text = t(user.id, "force_join") + "\n\n"
            for username, _ in FORCE_JOIN_CHANNELS:
                text += f"{username}\n"
            await query.edit_message_text(text, reply_markup=force_join_keyboard(user.id))
            return
        await query.edit_message_text(t(user.id, "choose_area"), reply_markup=main_menu_keyboard(user.id))
        return

    if data == "change_lang":
        await query.edit_message_text(TEXTS["ps"]["choose_lang"], reply_markup=lang_keyboard())
        return

    if data == "check_force_join":
        joined = await check_force_join_all(context.bot, user.id)
        if not joined:
            await query.answer(t(user.id, "join_failed"), show_alert=True)
            return
        await query.edit_message_text(t(user.id, "choose_area"), reply_markup=main_menu_keyboard(user.id))
        return

    if data == "back_main_menu":
        await query.edit_message_text(t(user.id, "main_menu") + "\n\n" + t(user.id, "choose_area"), reply_markup=main_menu_keyboard(user.id))
        return

    if data == "open_worker_menu":
        set_role(user.id, "worker")
        await process_user_leave_penalties(context.bot, user.id)
        await query.edit_message_text(t(user.id, "main_menu"), reply_markup=worker_menu(user.id))
        return

    if data == "open_client_menu":
        set_role(user.id, "client")
        await query.edit_message_text(t(user.id, "main_menu"), reply_markup=client_menu(user.id))
        return

    if data == "worker_balance":
        await process_user_leave_penalties(context.bot, user.id)
        await query.edit_message_text(
            t(user.id, "balance", balance=get_balance(user.id), debt=get_debt(user.id)),
            reply_markup=worker_menu(user.id),
        )
        return

    if data == "worker_bonus":
        row = get_user(user.id)
        last_bonus = row.get("last_bonus_at") if row else None
        if last_bonus:
            try:
                last_dt = datetime.fromisoformat(last_bonus)
                if datetime.now(timezone.utc) - last_dt < timedelta(hours=24):
                    await query.edit_message_text(t(user.id, "bonus_wait"), reply_markup=worker_menu(user.id))
                    return
            except Exception:
                pass
        cleared, added = add_reward_with_debt_clear(user.id, DAILY_BONUS_AFN)
        execute("UPDATE users SET last_bonus_at = %s WHERE user_id = %s", (now_iso(), user.id))
        await query.edit_message_text(
            t(user.id, "bonus_added") + f"\nقرض خلاص: {cleared} AFN\nاضافه: {added} AFN",
            reply_markup=worker_menu(user.id),
        )
        return

    if data == "worker_referral":
        await query.edit_message_text(
            t(user.id, "referral", link=referral_link(user.id), count=referral_count(user.id)),
            reply_markup=worker_menu(user.id),
        )
        return

    if data == "worker_tasks":
        await process_user_leave_penalties(context.bot, user.id)
        campaigns = fetch_all(
            """
            SELECT * FROM campaigns
            WHERE status = 'active'
              AND id NOT IN (
                  SELECT campaign_id FROM user_campaigns WHERE user_id = %s
              )
            ORDER BY id DESC
            """,
            (user.id,),
        )
        if not campaigns:
            await query.edit_message_text(t(user.id, "tasks_empty"), reply_markup=worker_menu(user.id))
            return
        await query.edit_message_text(t(user.id, "task_list_title"), reply_markup=campaign_list_keyboard(user.id, campaigns))
        return

    if data.startswith("open_campaign_"):
        campaign_id = int(data.split("_")[-1])
        c = get_campaign(campaign_id)
        if not c:
            return
        title = c["title_ps"] if user_lang(user.id) == "ps" else c["title_en"]
        await query.edit_message_text(
            f"{title}\n\n💰 Reward: {c['reward_afn']} AFN",
            reply_markup=task_card_keyboard(user.id, campaign_id, c["link"]),
        )
        return

    if data.startswith("verify_campaign_"):
        campaign_id = int(data.split("_")[-1])
        c = get_campaign(campaign_id)
        if not c:
            return

        existing = fetch_one(
            "SELECT * FROM user_campaigns WHERE user_id = %s AND campaign_id = %s",
            (user.id, campaign_id),
        )
        if existing:
            await query.edit_message_text(t(user.id, "task_already"), reply_markup=worker_menu(user.id))
            return

        ok = await check_join(context.bot, c["chat_username"], user.id)
        if not ok:
            await query.edit_message_text(t(user.id, "task_fail"), reply_markup=worker_menu(user.id))
            return

        owner_id = c["owner_user_id"]
        reward = int(c["reward_afn"])
        owner_balance = get_balance(owner_id)
        if owner_balance < reward:
            execute("UPDATE campaigns SET status = 'stopped' WHERE id = %s", (campaign_id,))
            await query.edit_message_text(t(user.id, "task_owner_low"), reply_markup=worker_menu(user.id))
            return

        add_balance(owner_id, -reward)
        cleared, added = add_reward_with_debt_clear(user.id, reward)
        execute(
            """
            INSERT INTO user_campaigns (user_id, campaign_id, reward_given, penalty_applied, owner_user_id, reward_afn, status, created_at, last_checked_at)
            VALUES (%s, %s, 1, 0, %s, %s, 'completed', %s, %s)
            """,
            (user.id, campaign_id, owner_id, reward, now_iso(), now_iso()),
        )
        execute("UPDATE campaigns SET completed_count = completed_count + 1 WHERE id = %s", (campaign_id,))

        await query.edit_message_text(
            t(user.id, "task_done", cleared=cleared, added=added),
            reply_markup=worker_menu(user.id),
        )
        return

    if data == "worker_withdraw":
        context.user_data["flow"] = "withdraw"
        context.user_data["withdraw_step"] = "amount"
        await query.message.reply_text(t(user.id, "withdraw_ask_amount"))
        return

    if data == "client_balance":
        await query.edit_message_text(
            t(user.id, "balance", balance=get_balance(user.id), debt=get_debt(user.id)),
            reply_markup=client_menu(user.id),
        )
        return

    if data == "client_bonus":
        row = get_user(user.id)
        last_bonus = row.get("last_bonus_at") if row else None
        if last_bonus:
            try:
                last_dt = datetime.fromisoformat(last_bonus)
                if datetime.now(timezone.utc) - last_dt < timedelta(hours=24):
                    await query.edit_message_text(t(user.id, "bonus_wait"), reply_markup=client_menu(user.id))
                    return
            except Exception:
                pass
        cleared, added = add_reward_with_debt_clear(user.id, DAILY_BONUS_AFN)
        execute("UPDATE users SET last_bonus_at = %s WHERE user_id = %s", (now_iso(), user.id))
        await query.edit_message_text(
            t(user.id, "bonus_added") + f"\nقرض خلاص: {cleared} AFN\nاضافه: {added} AFN",
            reply_markup=client_menu(user.id),
        )
        return

    if data == "client_referral":
        await query.edit_message_text(
            t(user.id, "referral", link=referral_link(user.id), count=referral_count(user.id)),
            reply_markup=client_menu(user.id),
        )
        return

    if data == "client_deposit":
        await query.edit_message_text(t(user.id, "deposit_menu"), reply_markup=deposit_method_keyboard(user.id))
        return

    if data == "deposit_method_hesab":
        context.user_data["flow"] = "deposit"
        context.user_data["deposit_step"] = "amount"
        context.user_data["deposit_method"] = "hesab"
        await query.message.reply_text(t(user.id, "deposit_hesab_info", number=HESAB_PAY))
        await query.message.reply_text(t(user.id, "deposit_amount_prompt"))
        return

    if data == "deposit_method_atoma":
        context.user_data["flow"] = "deposit"
        context.user_data["deposit_step"] = "amount"
        context.user_data["deposit_method"] = "atoma"
        await query.message.reply_text(t(user.id, "deposit_atoma_info", number=ATOMA_PAY))
        await query.message.reply_text(t(user.id, "deposit_amount_prompt"))
        return

    if data == "deposit_method_binance":
        context.user_data["flow"] = "deposit"
        context.user_data["deposit_step"] = "amount"
        context.user_data["deposit_method"] = "binance"
        await query.message.reply_text(t(user.id, "deposit_binance_info", uid=BINANCE_UID))
        await query.message.reply_text("USDT amount ولیکئ:")
        return

    if data == "client_add_task":
        if get_balance(user.id) < MIN_TASK_ADD_BALANCE:
            await query.edit_message_text(t(user.id, "need_min_task_balance"), reply_markup=client_menu(user.id))
            return
        await query.edit_message_text(t(user.id, "choose_task_type"), reply_markup=choose_task_type_keyboard(user.id))
        return

    if data == "task_type_channel":
        context.user_data["flow"] = "create_campaign"
        context.user_data["campaign_type"] = "channel"
        context.user_data["campaign_step"] = "link"
        await query.message.reply_text(t(user.id, "task_guide"))
        return

    if data == "task_type_group":
        context.user_data["flow"] = "create_campaign"
        context.user_data["campaign_type"] = "group"
        context.user_data["campaign_step"] = "link"
        await query.message.reply_text(t(user.id, "task_guide"))
        return

    if data == "client_campaigns":
        rows = fetch_all("SELECT * FROM campaigns WHERE owner_user_id = %s ORDER BY id DESC", (user.id,))
        if not rows:
            await query.edit_message_text(t(user.id, "campaigns_empty"), reply_markup=client_menu(user.id))
            return
        lines = []
        for r in rows:
            title = r["title_ps"] if user_lang(user.id) == "ps" else r["title_en"]
            lines.append(f"• {title}\nReward: {r['reward_afn']} AFN | Done: {r['completed_count']} | Status: {r['status']}")
        await query.edit_message_text("\n\n".join(lines), reply_markup=client_menu(user.id))
        return

    if data.startswith("admin_approve_deposit_"):
        if user.id != ADMIN_ID:
            return
        dep_id = int(data.split("_")[-1])
        dep = fetch_one("SELECT * FROM deposits WHERE id = %s", (dep_id,))
        if not dep or dep["status"] != "pending":
            return

        if dep["method"] == "binance":
            usdt_amount = float(dep.get("usdt_amount") or 0)
            add_amount = int(usdt_amount * USDT_RATE_AFN)
            fee_amount = 0
        else:
            add_amount, fee_amount = deposit_fee(int(dep["amount"]))

        add_balance(dep["user_id"], add_amount)
        execute("UPDATE deposits SET status = 'approved' WHERE id = %s", (dep_id,))

        dep_user = get_user(dep["user_id"])
        if dep_user and dep_user.get("referrer_id") and int(dep_user.get("referral_paid", 0)) == 0:
            add_balance(dep_user["referrer_id"], REFERRAL_REWARD_AFN)
            execute("UPDATE users SET referral_paid = 1 WHERE user_id = %s", (dep["user_id"],))

        if dep.get("channel_message_id"):
            if dep["method"] == "binance":
                channel_text = (
                    f"💳 Deposit Request #{dep_id}\n\n"
                    f"👤 User ID: {dep['user_id']}\n"
                    f"💸 {dep['usdt_amount']} USDT\n"
                    f"✅ Status: Completed"
                )
            else:
                channel_text = (
                    f"💳 Deposit Request #{dep_id}\n\n"
                    f"👤 User ID: {dep['user_id']}\n"
                    f"💰 {dep['amount']} AFN\n"
                    f"✅ Status: Completed"
                )
            try:
                await context.bot.edit_message_text(chat_id=PAYMENT_CHANNEL, message_id=dep["channel_message_id"], text=channel_text)
            except Exception:
                pass

        await context.bot.send_message(dep["user_id"], t(dep["user_id"], "approved_deposit"))
        return

    if data.startswith("admin_reject_deposit_"):
        if user.id != ADMIN_ID:
            return
        dep_id = int(data.split("_")[-1])
        dep = fetch_one("SELECT * FROM deposits WHERE id = %s", (dep_id,))
        if not dep or dep["status"] != "pending":
            return
        execute("UPDATE deposits SET status = 'rejected' WHERE id = %s", (dep_id,))
        if dep.get("channel_message_id"):
            try:
                await context.bot.edit_message_text(
                    chat_id=PAYMENT_CHANNEL,
                    message_id=dep["channel_message_id"],
                    text=f"💳 Deposit Request #{dep_id}\n\n👤 User ID: {dep['user_id']}\n❌ Status: Rejected",
                )
            except Exception:
                pass
        await context.bot.send_message(dep["user_id"], t(dep["user_id"], "rejected"))
        return

    if data.startswith("admin_approve_withdraw_"):
        if user.id != ADMIN_ID:
            return
        wd_id = int(data.split("_")[-1])
        wd = fetch_one("SELECT * FROM withdrawals WHERE id = %s", (wd_id,))
        if not wd or wd["status"] != "pending":
            return
        execute("UPDATE withdrawals SET status = 'approved' WHERE id = %s", (wd_id,))
        if wd.get("channel_message_id"):
            try:
                await context.bot.edit_message_text(
                    chat_id=PAYMENT_CHANNEL,
                    message_id=wd["channel_message_id"],
                    text=(
                        f"💸 Withdraw Request #{wd_id}\n\n"
                        f"👤 Name: {wd['full_name']}\n"
                        f"📱 Phone: {wd['phone']}\n"
                        f"🌐 Network: {wd['network']}\n"
                        f"💰 Amount: {wd['amount']} AFN\n\n"
                        f"✅ Status: Completed"
                    ),
                )
            except Exception:
                pass
        await context.bot.send_message(wd["user_id"], t(wd["user_id"], "approved_withdraw"))
        return

    if data.startswith("admin_reject_withdraw_"):
        if user.id != ADMIN_ID:
            return
        wd_id = int(data.split("_")[-1])
        wd = fetch_one("SELECT * FROM withdrawals WHERE id = %s", (wd_id,))
        if not wd or wd["status"] != "pending":
            return
        add_balance(wd["user_id"], int(wd["amount"]))
        execute("UPDATE withdrawals SET status = 'rejected' WHERE id = %s", (wd_id,))
        if wd.get("channel_message_id"):
            try:
                await context.bot.edit_message_text(
                    chat_id=PAYMENT_CHANNEL,
                    message_id=wd["channel_message_id"],
                    text=(
                        f"💸 Withdraw Request #{wd_id}\n\n"
                        f"👤 Name: {wd['full_name']}\n"
                        f"📱 Phone: {wd['phone']}\n"
                        f"🌐 Network: {wd['network']}\n"
                        f"💰 Amount: {wd['amount']} AFN\n\n"
                        f"❌ Status: Rejected"
                    ),
                )
            except Exception:
                pass
        await context.bot.send_message(wd["user_id"], t(wd["user_id"], "rejected"))
        return


# =========================
# TEXT FLOW
# =========================
async def flow_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.username or "", user.full_name or "")
    text = (update.message.text or "").strip()

    # quick text menu support
    if text in ("/balance", "💰 زما بیلانس", "💰 My Balance"):
        await update.message.reply_text(t(user.id, "balance", balance=get_balance(user.id), debt=get_debt(user.id)))
        return

    if text in ("/tasks", "📢 دندې", "📢 Tasks"):
        campaigns = fetch_all(
            """
            SELECT * FROM campaigns
            WHERE status = 'active'
              AND id NOT IN (SELECT campaign_id FROM user_campaigns WHERE user_id = %s)
            ORDER BY id DESC
            """,
            (user.id,),
        )
        if not campaigns:
            await update.message.reply_text(t(user.id, "tasks_empty"))
        else:
            await update.message.reply_text(t(user.id, "task_list_title"), reply_markup=campaign_list_keyboard(user.id, campaigns))
        return

    flow = context.user_data.get("flow")
    if not flow:
        return

    if flow == "withdraw":
        step = context.user_data.get("withdraw_step")

        if step == "amount":
            try:
                amount = int(update.message.text.strip())
            except Exception:
                await update.message.reply_text("Enter a valid amount")
                return
            if amount < MIN_WITHDRAW_AFN:
                await update.message.reply_text(t(user.id, "withdraw_min"))
                return
            if get_balance(user.id) < amount:
                await update.message.reply_text(t(user.id, "withdraw_low_balance"))
                context.user_data.clear()
                return
            context.user_data["withdraw_amount"] = amount
            context.user_data["withdraw_step"] = "name"
            await update.message.reply_text(t(user.id, "withdraw_ask_name"))
            return

        if step == "name":
            context.user_data["withdraw_name"] = text
            context.user_data["withdraw_step"] = "phone"
            await update.message.reply_text(t(user.id, "withdraw_ask_phone"))
            return

        if step == "phone":
            context.user_data["withdraw_phone"] = text
            context.user_data["withdraw_step"] = "network"
            await update.message.reply_text(t(user.id, "withdraw_ask_network"))
            return

        if step == "network":
            amount = int(context.user_data["withdraw_amount"])
            full_name = context.user_data["withdraw_name"]
            phone = context.user_data["withdraw_phone"]
            network = text

            add_balance(user.id, -amount)
            wd = execute(
                """
                INSERT INTO withdrawals (user_id, amount, network, phone, full_name, status, created_at)
                VALUES (%s, %s, %s, %s, %s, 'pending', %s)
                RETURNING id
                """,
                (user.id, amount, network, phone, full_name, now_iso()),
                returning=True,
            )
            wd_id = wd["id"]
            final_amount, fee_amount = withdraw_fee(amount)

            buttons_markup = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Approve", callback_data=f"admin_approve_withdraw_{wd_id}"),
                    InlineKeyboardButton("❌ Reject", callback_data=f"admin_reject_withdraw_{wd_id}"),
                ]
            ])

            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    f"💸 Withdraw Request #{wd_id}\n\n"
                    f"User: {full_name}\n"
                    f"Username: @{user.username or 'N/A'}\n"
                    f"User ID: {user.id}\n"
                    f"Requested Amount: {amount} AFN\n"
                    f"Fee: {fee_amount} AFN\n"
                    f"Send Amount: {final_amount} AFN\n"
                    f"Phone: {phone}\n"
                    f"Network: {network}"
                ),
                reply_markup=buttons_markup,
            )

            channel_msg = await context.bot.send_message(
                chat_id=PAYMENT_CHANNEL,
                text=(
                    f"💸 Withdraw Request #{wd_id}\n\n"
                    f"👤 Name: {full_name}\n"
                    f"📱 Phone: {phone}\n"
                    f"🌐 Network: {network}\n"
                    f"💰 Amount: {amount} AFN\n\n"
                    f"⏳ Status: Pending"
                ),
            )

            execute("UPDATE withdrawals SET channel_message_id = %s WHERE id = %s", (channel_msg.message_id, wd_id))
            await update.message.reply_text(t(user.id, "withdraw_sent"))
            context.user_data.clear()
            return

    if flow == "deposit":
        step = context.user_data.get("deposit_step")
        method = context.user_data.get("deposit_method")

        if step == "amount":
            try:
                if method == "binance":
                    usdt_amount = float(text)
                    if usdt_amount < MIN_USDT_DEPOSIT:
                        await update.message.reply_text(f"Minimum is {MIN_USDT_DEPOSIT} USDT")
                        return
                    amount_afn = int(usdt_amount * USDT_RATE_AFN)
                    context.user_data["deposit_usdt_amount"] = usdt_amount
                    context.user_data["deposit_amount"] = amount_afn
                else:
                    amount_afn = int(text)
                    if amount_afn < MIN_DEPOSIT_AFN:
                        await update.message.reply_text(f"Minimum deposit is {MIN_DEPOSIT_AFN} AFN")
                        return
                    context.user_data["deposit_amount"] = amount_afn
            except Exception:
                await update.message.reply_text("Enter a valid amount")
                return
            context.user_data["deposit_step"] = "proof"
            await update.message.reply_text(t(user.id, "deposit_proof_prompt"))
            return

        if step == "proof":
            if not update.message.photo:
                await update.message.reply_text(t(user.id, "deposit_proof_prompt"))
                return

            amount = int(context.user_data["deposit_amount"])
            proof_file_id = update.message.photo[-1].file_id
            usdt_amount = context.user_data.get("deposit_usdt_amount")

            dep = execute(
                """
                INSERT INTO deposits (user_id, amount, method, proof_file_id, usdt_amount, status, created_at)
                VALUES (%s, %s, %s, %s, %s, 'pending', %s)
                RETURNING id
                """,
                (user.id, amount, method, proof_file_id, usdt_amount, now_iso()),
                returning=True,
            )
            dep_id = dep["id"]

            buttons_markup = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Approve", callback_data=f"admin_approve_deposit_{dep_id}"),
                    InlineKeyboardButton("❌ Reject", callback_data=f"admin_reject_deposit_{dep_id}"),
                ]
            ])

            if method == "binance":
                caption = (
                    f"💳 Deposit Request #{dep_id}\n\n"
                    f"User: {user.full_name}\n"
                    f"Username: @{user.username or 'N/A'}\n"
                    f"User ID: {user.id}\n"
                    f"Method: Binance\n"
                    f"USDT: {usdt_amount}\n"
                    f"Add to Balance: {amount} AFN"
                )
                channel_text = (
                    f"💳 Deposit Request #{dep_id}\n\n"
                    f"👤 User ID: {user.id}\n"
                    f"💸 {usdt_amount} USDT\n"
                    f"⏳ Status: Pending"
                )
            else:
                final_amount, fee_amount = deposit_fee(amount)
                caption = (
                    f"💳 Deposit Request #{dep_id}\n\n"
                    f"User: {user.full_name}\n"
                    f"Username: @{user.username or 'N/A'}\n"
                    f"User ID: {user.id}\n"
                    f"Method: {method}\n"
                    f"Amount: {amount} AFN\n"
                    f"Fee: {fee_amount} AFN\n"
                    f"Add to Balance: {final_amount} AFN"
                )
                channel_text = (
                    f"💳 Deposit Request #{dep_id}\n\n"
                    f"👤 User ID: {user.id}\n"
                    f"💰 {amount} AFN\n"
                    f"⏳ Status: Pending"
                )

            await context.bot.send_photo(chat_id=ADMIN_ID, photo=proof_file_id, caption=caption, reply_markup=buttons_markup)
            channel_msg = await context.bot.send_message(chat_id=PAYMENT_CHANNEL, text=channel_text)
            execute("UPDATE deposits SET channel_message_id = %s WHERE id = %s", (channel_msg.message_id, dep_id))
            await update.message.reply_text(t(user.id, "deposit_sent"))
            context.user_data.clear()
            return

    if flow == "create_campaign":
        step = context.user_data.get("campaign_step")
        campaign_type = context.user_data.get("campaign_type")

        if step == "link":
            link = text
            chat_username = extract_chat_username(link)
            if not chat_username:
                await update.message.reply_text(t(user.id, "invalid_link"))
                return
            context.user_data["campaign_link"] = link
            context.user_data["campaign_chat_username"] = chat_username
            context.user_data["campaign_step"] = "title"
            await update.message.reply_text(t(user.id, "ask_title"))
            return

        if step == "title":
            title = text
            link = context.user_data["campaign_link"]
            chat_username = context.user_data["campaign_chat_username"]
            execute(
                """
                INSERT INTO campaigns (owner_user_id, title_ps, title_en, reward_afn, target_type, link, channel_title, chat_username, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'active', %s)
                """,
                (user.id, f"{title} جوین کړئ", f"Join {title}", TASK_REWARD_AFN, campaign_type, link, title, chat_username, now_iso()),
            )
            users = fetch_all("SELECT user_id FROM users")
            for u in users:
                try:
                    await context.bot.send_message(chat_id=u["user_id"], text=t(u["user_id"], "new_task_broadcast"))
                except Exception:
                    pass
            await update.message.reply_text(t(user.id, "campaign_created"))
            context.user_data.clear()
            return


# =========================
# MAIN
# =========================
def main():
    init_db()
    ensure_admin_account()

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(buttons))
    app.add_handler(MessageHandler((filters.TEXT | filters.PHOTO) & ~filters.COMMAND, flow_router))

    app.job_queue.run_repeating(periodic_leave_check, interval=AUTO_LEAVE_PENALTY_CHECK_HOURS * 3600, first=300)
    app.job_queue.run_repeating(daily_promo_post, interval=86400, first=60)

    logger.info("EasyEarn full pro bot is running...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
