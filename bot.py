import logging
import os
import re
from collections import defaultdict
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

FORCE_JOIN_CHANNELS =[
    ("@easyearnofficial1222", "https://t.me/easyearnofficial1222"),
    ("@easyearnpayments", "https://t.me/easyearnpayments"),
    ("@easyearnu", "https://t.me/easyearnu"),
]

REFERRAL_PERCENT = Decimal("15")
DAILY_BONUS_STARS = Decimal("0.5")
WITHDRAW_OPTIONS =[Decimal("15"), Decimal("25"), Decimal("50")]
BONUS_INTERVAL_HOURS = 24
PROMO_INTERVAL_HOURS = 24
LEAVE_CHECK_INTERVAL_HOURS = 2
WITHDRAW_COOLDOWN_HOURS = 1.5
DEFAULT_PAGE_SIZE = 8

PROMO_TEXT = (
    "📢 زمونږ خدمات\n\n"
    "⭐ د تلیګرام پریمیم او ستوري اخیستل\n"
    "📢 د تلیګرام اعلان\n"
    "📘 د فیسبوک او انسټاګرام اعلانونه\n"
    "📱 د خارجي ویرچول نمبرونه اخیستل\n\n"
    f"📩 د ترلاسه کولو لپاره لاندي آيډي ته مسج وکړئ:\n{SUPPORT_USERNAME}"
)

# In-memory message counter for promo
promo_msg_counter = defaultdict(int)

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
        "choose_lang": "ژبه انتخاب کړئ:", "intro": "ښه راغلاست EasyEarn Bot ته", "force_join": "مهرباني وکړئ ټول چینلونه جوین کړئ:",
        "joined_btn": "✅ جوین مې کړل", "join_failed": "اول ټول اړین چینلونه جوین کړئ.", "my_stars": "⭐ ستاسو ستوري: {stars}",
        "referral": "👥 ستاسو ریفرل لینک:\n{link}\n\nتاسو به د خپلو ریفرلونو له اعتبار لرونکي عاید څخه 15% ترلاسه کوئ.\nټول ریفرلونه: {count}",
        "tasks_empty": "❌ فعلاً هېڅ تاسک نشته", "task_done": "✅ تاسک بشپړ شو\n⭐ {stars}", "task_already": "تاسو دا تاسک مخکې بشپړ کړی",
        "task_fail": "❌ لومړی چینل/ګروپ جوین کړئ، بیا تایید وکړئ", "task_bot_fail": "❌ د دې bot task دقیق اتومات تایید ممکن نه دی. screenshot proof ولېږئ.",
        "bonus_added": "✅ ورځنی بونس واخیستل شو: {stars} ⭐", "bonus_wait": "⏳ بونس مخکې اخیستل شوی. پاتې وخت: {remaining}",
        "withdraw_choose": "💸 د ویډرا لپاره انتخاب وکړئ:", "withdraw_low": "❌ بیلانس کم دی", "withdraw_cooldown": "⏳ لا ویډرا نه شي کېدای. د انتظار پاتې وخت: {remaining}",
        "withdraw_no_username": "❌ لومړی خپل ټیلیګرام یوزرنیم وټاکئ، بیا د ویډرا غوښتنه وکړئ.",
        "withdraw_sent": "✅ ستاسو د ویډرا غوښتنه ثبت شوه.\n⏳ ستاسو ستوري به تر ۱۲ ساعتونو پورې درورسیږي.",
        "about": "ℹ️ زمونږ په اړه\n\nEasyEarn Bot د تاسکونو، ریفرلونو او ورځني بونس له لارې د ستورو ګټلو سیستم دی.",
        "support": "📞 سپورټ\n\nمهرباني وکړئ دې یوزرنیم ته مسج وکړئ:\n{username}",
        "stats_admin": "👥 ټول یوزران: {users}\n🆕 د نن یوزران: {today}\n⭐ د ټولو یوزرانو ستوري: {stars}\n⭐ د اډمین ستوري: {admin_stars}\n📝 فعال تاسکونه: {tasks}",
        "admin_only": "دا برخه یوازې اډمین ته ده.", "admin_help": "🛠 Admin Commands\n\n/users\n/refstats\n/withdraws\n/botstats\n/taskslist\n/taskstats\n/ban USER_ID [reason]\n/unban USER_ID",
        "broadcast_prompt": "هغه مسج ولیکئ چې ټولو users ته ولاړ شي.", "addtask_kind": "د task ډول انتخاب کړئ:",
        "addtask_link": "د چینل/ګروپ لینک یا @username راولېږئ.", "addtask_title": "د task عنوان راولېږئ.",
        "addtask_reward": "ریوارډ ولیکئ، مثال: 0.5", "addtask_post_link": "د post لینک راولېږئ.",
        "addtask_bot_link": "د bot لینک راولېږئ. مثال: https://t.me/SomeBot?start=abc",
        "addbalance_prompt": "هغه stars ولیکئ چې اډمین بیلانس ته اضافه شي. مثال: 1000",
        "addbalance_done": "✅ اډمین بیلانس {amount} stars سره زیات شو.\n⭐ نوی بیلانس: {new_balance}",
        "removetask_prompt": "د لرې کولو لپاره تاسک انتخاب کړئ.", "cancelled": "❌ عمل لغوه شو.",
        "open_task_btn": "🔗 تاسک خلاص کړه", "verify_btn": "✅ تایید", "send_proof_btn": "📸 proof ولېږه",
        "leave_notice": "⚠️ تاسو یو rewarded چینل/ګروپ پرېښود. ستاسو reward بېرته کم شو او task بیا فعال شو.",
        "new_withdraw": "📤 د ویډرا نوې غوښتنه!", "proof_prompt": "📸 مهرباني وکړئ screenshot proof همدا اوس راولېږئ.",
        "proof_saved": "✅ proof واستول شو. د اډمین تایید ته منتظر اوسئ.", "proof_rejected": "❌ ستاسو proof رد شو. task بیا درته ښکاره شو.",
        "proof_approved": "✅ ستاسو proof منظور شو.\n⭐ {stars}", "banned": "⛔ ستاسو اکاونټ بند شوی. د مرستې لپاره سپورټ سره اړیکه ونیسئ.",
        "all_tasks_done": "✅ ټول موجود تاسکونه دې بشپړ کړي", "withdraw_support_no_username": "⚠️ ستا username نشته. د ویډرا لپاره سپورټ سره هم اړیکه ونیسه: {username}",
    },
    "en": {
        "choose_lang": "Choose language:", "intro": "Welcome to EasyEarn Bot", "force_join": "Please join all required channels first:",
        "joined_btn": "✅ I Joined", "join_failed": "Please join all required channels first.", "my_stars": "⭐ Your stars: {stars}",
        "referral": "👥 Your referral link:\n{link}\n\nYou earn 15% only from valid completed task rewards of your referrals.\nTotal referrals: {count}",
        "tasks_empty": "❌ No tasks available right now.", "task_done": "✅ Task completed\n⭐ {stars}", "task_already": "You already completed this task.",
        "task_fail": "❌ Join the channel/group first, then verify.", "task_bot_fail": "❌ Exact automatic verification is not possible for this bot task. Please send screenshot proof.",
        "bonus_added": "✅ Daily bonus claimed: {stars} ⭐", "bonus_wait": "⏳ Bonus already claimed. Remaining: {remaining}",
        "withdraw_choose": "💸 Choose your withdrawal option:", "withdraw_low": "❌ Insufficient balance.", "withdraw_cooldown": "⏳ Withdrawal is locked for now. Remaining wait: {remaining}",
        "withdraw_no_username": "❌ Please set a Telegram username first, then request withdraw.",
        "withdraw_sent": "✅ Your withdraw request was received.\n⏳ Your stars will arrive within 12 hours.",
        "about": "ℹ️ About Us\n\nEasyEarn Bot is a stars earning system through tasks, referrals, and daily bonus.",
        "support": "📞 Support\n\nPlease message:\n{username}",
        "stats_admin": "👥 Total users: {users}\n🆕 Today users: {today}\n⭐ Total user stars: {stars}\n⭐ Admin stars: {admin_stars}\n📝 Active tasks: {tasks}",
        "admin_only": "This section is admin only.", "admin_help": "🛠 Admin Commands\n\n/users\n/refstats\n/withdraws\n/botstats\n/taskslist\n/taskstats\n/ban USER_ID [reason]\n/unban USER_ID",
        "broadcast_prompt": "Send the message you want to broadcast.", "addtask_kind": "Choose task type:",
        "addtask_link": "Send channel/group link or @username.", "addtask_title": "Send task title.",
        "addtask_reward": "Send reward, example: 0.5", "addtask_post_link": "Send the post link.",
        "addtask_bot_link": "Send the bot link. Example: https://t.me/SomeBot?start=abc",
        "addbalance_prompt": "Send stars amount to add to admin balance. Example: 1000",
        "addbalance_done": "✅ Admin balance increased by {amount} stars.\n⭐ New balance: {new_balance}",
        "removetask_prompt": "Choose a task to remove.", "cancelled": "❌ Action cancelled.",
        "open_task_btn": "🔗 Open Task", "verify_btn": "✅ Verify", "send_proof_btn": "📸 Send proof",
        "leave_notice": "⚠️ You left a rewarded channel/group. Your reward was deducted and the task became active again.",
        "new_withdraw": "📤 New withdrawal request!", "proof_prompt": "📸 Please send screenshot proof now.",
        "proof_saved": "✅ Proof received. Waiting for admin review.", "proof_rejected": "❌ Your proof was rejected. The task is available again.",
        "proof_approved": "✅ Your proof was approved.\n⭐ {stars}", "banned": "⛔ Your account is banned. Contact support for help.",
        "all_tasks_done": "✅ You have completed all available tasks", "withdraw_support_no_username": "⚠️ You do not have a username. Also contact support for withdrawal: {username}",
    },
    "fa": {
        "choose_lang": "زبان را انتخاب کنید:", "intro": "به ربات EasyEarn خوش آمدید", "force_join": "لطفاً ابتدا در همه کانال‌های مورد نیاز عضو شوید:",
        "joined_btn": "✅ عضو شدم", "join_failed": "لطفاً ابتدا در همه کانال‌های مورد نیاز عضو شوید.", "my_stars": "⭐ ستاره‌های شما: {stars}",
        "referral": "👥 لینک زیرمجموعه‌گیری شما:\n{link}\n\nشما 15% از درآمد حاصل از تسک‌های معتبر زیرمجموعه‌های خود را دریافت می‌کنید.\nتعداد کل زیرمجموعه‌ها: {count}",
        "tasks_empty": "❌ فعلاً تسکی در دسترس نیست.", "task_done": "✅ تسک با موفقیت انجام شد\n⭐ {stars}", "task_already": "شما قبلاً این تسک را انجام داده‌اید.",
        "task_fail": "❌ ابتدا در کانال/گروه عضو شوید، سپس تایید کنید.", "task_bot_fail": "❌ تایید خودکار برای این تسک ربات امکان‌پذیر نیست. لطفاً اسکرین‌شات بفرستید.",
        "bonus_added": "✅ پاداش روزانه دریافت شد: {stars} ⭐", "bonus_wait": "⏳ پاداش قبلاً دریافت شده است. زمان باقی‌مانده: {remaining}",
        "withdraw_choose": "💸 گزینه برداشت را انتخاب کنید:", "withdraw_low": "❌ موجودی کافی نیست.", "withdraw_cooldown": "⏳ امکان برداشت در حال حاضر وجود ندارد. زمان باقی‌مانده: {remaining}",
        "withdraw_no_username": "❌ ابتدا یک نام کاربری تلگرام تنظیم کنید، سپس درخواست برداشت بدهید.",
        "withdraw_sent": "✅ درخواست برداشت شما ثبت شد.\n⏳ ستاره‌های شما تا ۱۲ ساعت آینده ارسال خواهد شد.",
        "about": "ℹ️ درباره ما\n\nEasyEarn Bot سیستمی برای کسب ستاره از طریق تسک‌ها، زیرمجموعه‌گیری و پاداش روزانه است.",
        "support": "📞 پشتیبانی\n\nلطفاً به آیدی زیر پیام دهید:\n{username}",
        "stats_admin": "👥 کل کاربران: {users}\n🆕 کاربران امروز: {today}\n⭐ مجموع ستاره کاربران: {stars}\n⭐ ستاره ادمین: {admin_stars}\n📝 تسک‌های فعال: {tasks}",
        "admin_only": "این بخش فقط مخصوص ادمین است.", "admin_help": "🛠 دستورات ادمین\n\n/users\n/refstats\n/withdraws\n/botstats\n/taskslist\n/taskstats\n/ban USER_ID [reason]\n/unban USER_ID",
        "broadcast_prompt": "پیامی که می‌خواهید ارسال کنید را بنویسید.", "addtask_kind": "نوع تسک را انتخاب کنید:",
        "addtask_link": "لینک کانال/گروه یا نام کاربری را بفرستید.", "addtask_title": "عنوان تسک را بفرستید.",
        "addtask_reward": "پاداش را بنویسید، مثال: 0.5", "addtask_post_link": "لینک پست را بفرستید.",
        "addtask_bot_link": "لینک ربات را بفرستید. مثال: https://t.me/SomeBot?start=abc",
        "addbalance_prompt": "مقدار ستاره برای اضافه کردن به موجودی ادمین را بنویسید. مثال: 1000",
        "addbalance_done": "✅ موجودی ادمین به میزان {amount} افزایش یافت.\n⭐ موجودی جدید: {new_balance}",
        "removetask_prompt": "تسک مورد نظر برای حذف را انتخاب کنید.", "cancelled": "❌ عملیات لغو شد.",
        "open_task_btn": "🔗 باز کردن تسک", "verify_btn": "✅ تایید", "send_proof_btn": "📸 ارسال اثبات",
        "leave_notice": "⚠️ شما یک کانال/گروه پاداش‌دار را ترک کردید. پاداش شما کسر شد و تسک دوباره فعال شد.",
        "new_withdraw": "📤 درخواست برداشت جدید!", "proof_prompt": "📸 لطفاً اسکرین‌شات اثبات را همین الان بفرستید.",
        "proof_saved": "✅ اثبات دریافت شد. در انتظار تایید ادمین.", "proof_rejected": "❌ اثبات شما رد شد. تسک دوباره در دسترس است.",
        "proof_approved": "✅ اثبات شما تایید شد.\n⭐ {stars}", "banned": "⛔ حساب شما مسدود شده است. برای راهنمایی با پشتیبانی تماس بگیرید.",
        "all_tasks_done": "✅ شما همه تسک‌های موجود را انجام داده‌اید", "withdraw_support_no_username": "⚠️ شما نام کاربری ندارید. همچنین برای برداشت با پشتیبانی تماس بگیرید: {username}",
    }
}


def get_text(lang: str, key: str, **kwargs) -> str:
    return TEXTS.get(lang, TEXTS["ps"]).get(key, TEXTS["en"].get(key, "")).format(**kwargs)


# =====================================
# DB INIT
# =====================================
def init_db():
    execute("CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY, username TEXT, full_name TEXT, lang TEXT DEFAULT 'ps', stars NUMERIC(12,2) DEFAULT 0, referrer_id BIGINT, last_bonus_at TEXT, created_at TEXT, withdraw_eligible_at TEXT, is_banned BOOLEAN DEFAULT FALSE, banned_at TEXT, ban_reason TEXT, last_task_message_id BIGINT, last_task_chat_id BIGINT)")
    execute("CREATE TABLE IF NOT EXISTS tasks (id SERIAL PRIMARY KEY, task_type TEXT DEFAULT 'channel', channel_title TEXT NOT NULL, chat_username TEXT, link TEXT NOT NULL, reward_stars NUMERIC(12,2) DEFAULT 0.5, status TEXT DEFAULT 'active', created_at TEXT, requires_proof BOOLEAN DEFAULT FALSE, post_link TEXT, bot_link TEXT, metadata TEXT)")
    execute("CREATE TABLE IF NOT EXISTS user_tasks (id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, task_id INTEGER NOT NULL, rewarded_stars NUMERIC(12,2) DEFAULT 0, reward_removed INTEGER DEFAULT 0, status TEXT DEFAULT 'pending', created_at TEXT, completed_at TEXT, last_checked_at TEXT, proof_file_id TEXT, proof_file_unique_id TEXT, proof_message_id BIGINT, admin_review_message_id BIGINT, rejection_reason TEXT, suspicious INTEGER DEFAULT 0, UNIQUE(user_id, task_id))")
    execute("CREATE TABLE IF NOT EXISTS withdrawals (id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, amount NUMERIC(12,2) DEFAULT 0, amount_stars NUMERIC(12,2) DEFAULT 0, status TEXT DEFAULT 'pending', admin_message_id BIGINT, channel_message_id BIGINT, created_at TEXT, approved_at TEXT, rejected_at TEXT, reason TEXT)")
    execute("CREATE TABLE IF NOT EXISTS promo_chats (chat_id BIGINT PRIMARY KEY, title TEXT, chat_type TEXT, is_active INTEGER DEFAULT 1, created_at TEXT)")
    execute("CREATE TABLE IF NOT EXISTS referral_earnings (id SERIAL PRIMARY KEY, referrer_id BIGINT NOT NULL, referred_user_id BIGINT NOT NULL, task_id INTEGER NOT NULL, base_reward NUMERIC(12,2) NOT NULL, bonus_amount NUMERIC(12,2) NOT NULL, created_at TEXT NOT NULL, UNIQUE(referred_user_id, task_id))")
    
    admin = fetch_one("SELECT user_id FROM users WHERE user_id = %s", (ADMIN_ID,))
    if not admin:
        execute("INSERT INTO users (user_id, username, full_name, lang, stars, created_at) VALUES (%s, %s, %s, 'ps', %s, %s)", (ADMIN_ID, "admin", "Admin", ADMIN_START_STARS, now_iso()))


# =====================================
# CORE FUNCTIONS
# =====================================
def get_lang(user_id: int) -> str:
    row = fetch_one("SELECT lang FROM users WHERE user_id = %s", (user_id,))
    lang = (row or {}).get("lang") or "ps"
    return lang if lang in ("ps", "en", "fa") else "ps"

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

def add_stars(user_id: int, amount: Decimal) -> None:
    amount = decimalize(amount)
    execute("UPDATE users SET stars = COALESCE(stars, 0) + %s WHERE user_id = %s", (amount, user_id))

def update_withdraw_eligibility(user_id: int) -> None:
    min_w = min(WITHDRAW_OPTIONS)
    execute("UPDATE users SET withdraw_eligible_at = %s WHERE user_id = %s AND stars >= %s AND withdraw_eligible_at IS NULL", (now_iso(), user_id, min_w))

# =====================================
# UI ROUTING (Multilingual Fix)
# =====================================
def get_action_from_button(text: str) -> Optional[str]:
    # Mapping buttons across all supported languages (PS, EN, FA)
    mapping = {
        "withdraw":["🏧 Withdraw", "🏧 د پیسو ایستل", "🏧 برداشت"],
        "stars":["⭐ My Stars", "⭐ زما ستوري", "⭐ ستاره‌های من"],
        "referral":["👥 Referral", "👥 ریفرل", "👥 زیرمجموعه‌گیری"],
        "tasks": ["📝 Tasks", "📝 تاسکونه", "📝 تسک‌ها"],
        "bonus": ["🎁 Bonus", "🎁 ورځنی بونس", "🎁 پاداش"],
        "lang":["🌐 Language", "🌐 ژبه", "🌐 زبان"],
        "about":["ℹ️ About Us", "ℹ️ زمونږ په اړه", "ℹ️ درباره ما"],
        "support":["📞 Support", "📞 سپورټ", "📞 پشتیبانی"],
        "stats": ["📊 Statistics", "📊 احصایې", "📊 آمار"],
        "broadcast": ["📣 Broadcast", "📣 نشر", "📣 ارسال همگانی"],
        "add_task":["🛠 Add Task", "🛠 تاسک اضافه کول", "🛠 افزودن تسک"],
        "remove_task":["🗑 Remove Task", "🗑 تاسک لرې کول", "🗑 حذف تسک"],
        "add_balance":["➕ Add Balance", "➕ بیلانس زیاتول", "➕ افزودن موجودی"]
    }
    for action, texts in mapping.items():
        if text in texts: return action
    return None

def main_menu(user_id: int) -> ReplyKeyboardMarkup:
    lang = get_lang(user_id)
    # Reconstruct menu based on language
    btn = lambda key: get_text(lang, key) # This would require a separate key mapping
    # Simplified approach for existing structure:
    keyboard = [
        ["🏧 Withdraw", "⭐ My Stars"],["👥 Referral", "📝 Tasks"],
        ["🎁 Bonus", "🌐 Language"],
        ["ℹ️ About Us", "📞 Support"],
    ]
    if user_id == ADMIN_ID:
        keyboard.insert(0,["📊 Statistics", "📣 Broadcast"])
        keyboard.insert(1, ["🛠 Add Task", "🗑 Remove Task"])
        keyboard.insert(2,["➕ Add Balance"])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# =====================================
# HANDLERS (Improved)
# =====================================
async def guard_user_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    if not user: return False
    ensure_user(user.id, user.username or "", user.full_name or "")
    if is_banned(user.id):
        if update.message: await update.message.reply_text(t(user.id, "banned"))
        return False
    return True

async def user_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text: return
    if not await guard_user_access(update, context): return
    
    chat = update.effective_chat
    text = update.message.text
    user = update.effective_user

    # Promo logic (10 messages per chat)
    if chat.type in ['group', 'supergroup']:
        promo_msg_counter[chat.id] += 1
        if promo_msg_counter[chat.id] >= 10:
            promo_msg_counter[chat.id] = 0
            await update.message.reply_text(PROMO_TEXT)
        return

    # Routing
    action = get_action_from_button(text)
    
    if action == "withdraw":
        user_row = get_user(user.id)
        if not user_row or not user_row.get("username"):
            await update.message.reply_text(t(user.id, "withdraw_no_username"))
            return
        # ... existing logic ...
    elif action == "tasks":
        # ... existing logic ...
        pass
    # ... handle others ...
    else:
        await update.message.reply_text(t(user.id, "intro"), reply_markup=main_menu(user.id))

# =====================================
# PROOF HANDLING (Fixed)
# =====================================
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    if not data: return
    
    if data.startswith("proof_ok_"):
        record_id = int(data.split("_")[2])
        row = fetch_one("SELECT * FROM user_tasks WHERE id = %s AND status = 'pending_review'", (record_id,))
        if row:
            complete_exact_task_reward(int(row["user_id"]), int(row["task_id"]), Decimal(str(row["rewarded_stars"])))
            execute("UPDATE user_tasks SET status = 'completed' WHERE id = %s", (record_id,))
            try: await context.bot.send_message(int(row["user_id"]), t(int(row["user_id"]), "proof_approved", stars=pretty_amount(row["rewarded_stars"])))
            except: pass
            await query.message.edit_caption(caption="✅ Approved")
    elif data.startswith("proof_no_"):
        record_id = int(data.split("_")[2])
        execute("UPDATE user_tasks SET status = 'rejected' WHERE id = %s", (record_id,))
        row = fetch_one("SELECT user_id FROM user_tasks WHERE id = %s", (record_id,))
        if row:
            try: await context.bot.send_message(int(row["user_id"]), t(int(row["user_id"]), "proof_rejected"))
            except: pass
            await query.message.edit_caption(caption="❌ Rejected")
    await query.answer()

# =====================================
# MAIN
# =====================================
def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", lambda u, c: user_router(u, c)))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, user_router))
    logger.info("Bot started...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
