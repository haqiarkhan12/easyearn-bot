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
ADMIN_START_STARS = float(os.getenv("ADMIN_START_STARS", "100000"))

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
    "📢 زمونږ خدمات\n\n"
    "⭐ د تلیګرام ستوري\n"
    "📢 د چینل او پیج پروموشن\n"
    "⚡ چټک او باوري خدمات\n\n"
    "📢 زموږ چینلونه:\n"
    "🔗 https://t.me/haqyarserviceso1\n"
    "🔗 https://t.me/Solutions3232\n\n"
    f"📞 سپورټ: {SUPPORT_USERNAME}"
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
            referral_rewarded INTEGER DEFAULT 0,
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
        amount_stars NUMERIC(12,2) NOT NULL,
        status TEXT DEFAULT 'pending',
        channel_message_id BIGINT,
        created_at TEXT,
        completed_at TEXT
    )
    """
)

# FIX
    safe_exec("ALTER TABLE withdrawals ADD COLUMN IF NOT EXISTS amount_stars NUMERIC(12,2)")
    safe_exec("ALTER TABLE withdrawals ADD COLUMN IF NOT EXISTS amount NUMERIC(12,2)")
    safe_exec("ALTER TABLE withdrawals ALTER COLUMN amount DROP NOT NULL")
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
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_rewarded INTEGER DEFAULT 0")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_bonus_at TEXT")
    safe_exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TEXT")

    try:
        execute(
            """
            UPDATE users
            SET stars = CASE
                WHEN COALESCE(stars, 0) = 0 AND COALESCE(balance, 0) > 0 THEN ROUND((balance::numeric / 2), 2)
                ELSE stars
            END
            """
        )
    except Exception as e:
        logger.info("old balance migration skipped: %s", e)

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
        "choose_lang": "ژبه انتخاب کړئ:",
        "intro": "ښه راغلاست EasyEarn Bot ته\n\nپه دې بوټ کې تاسو کولی شئ د ټاسکونو په بشپړولو Stars ترلاسه کړئ، ورځنی بونس واخلئ، ریفرلونه راولئ، او وروسته خپل Stars Withdraw کړئ.",
        "force_join": "مهرباني وکړئ دواړه چینلونه جوین کړئ:",
        "joined_btn": "✅ جوین مې کړل",
        "join_failed": "اول دواړه چینلونه جوین کړئ.",
        "my_stars": "⭐ ستاسو ستوري: {stars}",
        "referral": "👥 ستاسو ریفرل لینک:\n{link}\n\nهر ریفرل په سر تاسو 1.25 ستوري ترلاسه کوئ.\nجعلي ریفرل نه منل کیږي، که وپیژندل شي ستاسو اکاونټ به بند شي.\n\nټول ریفرلونه: {count}",
        "tasks_empty": "فعلاً ټاسک نشته.",
        "task_done": "✅ ټاسک بشپړ شو.\n⭐ +{stars} ستوري اضافه شول.",
        "task_already": "تاسو دا ټاسک مخکې بشپړ کړی.",
        "task_fail": "اول چینل جوین کړئ، بیا Verify وکړئ.",
        "bonus_added": "🎁 ورځنی بونس اضافه شو.\n⭐ +1 ستوری",
        "bonus_wait": "⏳ تاسو بونس اخیستی. پاتې وخت: {remaining}",
        "withdraw_choose": "💸 د ویډرا لپاره انتخاب وکړئ:",
        "withdraw_low": "ستاسو ستوري کم دي.",
        "admin_low": "د اډمین ستوري کم دي. وروسته بیا هڅه وکړئ.",
        "withdraw_sent": "✅ ستاسو د ویډرا غوښتنه ثبت شوه.",
        "withdraw_failed": "ویډرا ونه لېږل شوه. ADMIN_ID او د چینل permissions وګورئ.",
        "about": "ℹ️ زمونږ په اړه\n\nزمونږ بوټ د Telegram Stars earning لپاره جوړ شوی. تاسو د ټاسکونو، ورځني بونس او ریفرلونو له لارې Stars ترلاسه کوئ.",
        "support": "📞 سپورټ\n\nمهرباني وکړئ دې یوزرنیم ته مسج وکړئ:\n{username}",
        "new_task": "📢 نوی ټاسک اضافه شو!\n⭐ Reward: {reward}",
        "stats_admin": "👥 ټول یوزران: {users}\n🆕 د نن یوزران: {today}\n⭐ د ټولو یوزرانو Stars: {stars}\n⭐ د اډمین Stars: {admin_stars}\n📝 فعال ټاسکونه: {tasks}",
        "admin_only": "دا برخه یوازې اډمین ته ده.",
        "admin_help": "🛠 Admin Commands\n\n/users\n/refstats\n/withdraws\n/botstats\n/broadcast\n/addtask\n/addbalance\n/taskslist\n/taskstats\n/removetask",
        "broadcast_prompt": "هغه مسج ولیکئ چې ټولو users ته ولاړ شي.",
        "addtask_link": "د چینل لینک یا @username راولېږئ.",
        "addtask_title": "د چینل عنوان راولېږئ.",
        "addtask_reward": "ریوارډ ولیکئ، مثال: 0.5",
        "addbalance_prompt": "هغه stars ولیکئ چې اډمین بیلانس ته اضافه شي. مثال: 1000",
        "addbalance_done": "✅ اډمین بیلانس {amount} stars سره زیات شو.\n⭐ نوی بیلانس: {new_balance}",
        "removetask_prompt": "د لرې کولو لپاره Task ID راولېږئ. مثال: 5",
        "cancelled": "❌ عمل لغوه شو.",
    },
    "en": {
        "choose_lang": "Choose language:",
        "intro": "Welcome to EasyEarn Bot\n\nIn this bot you can complete tasks, earn Stars, claim a daily bonus, invite referrals, and withdraw your Stars later.",
        "force_join": "Please join both channels first:",
        "joined_btn": "✅ I Joined",
        "join_failed": "Please join both channels first.",
        "my_stars": "⭐ Your stars: {stars}",
        "referral": "👥 Your referral link:\n{link}\n\nYou earn 1.25 stars per referral. Fake referrals are not accepted. If detected, your account may be banned.\n\nTotal referrals: {count}",
        "tasks_empty": "No tasks available.",
        "task_done": "✅ Task completed.\n⭐ +{stars} stars added.",
        "task_already": "You already completed this task.",
        "task_fail": "Join the channel first, then verify.",
        "bonus_added": "🎁 Daily bonus added.\n⭐ +1 star",
        "bonus_wait": "⏳ Bonus already claimed. Remaining: {remaining}",
        "withdraw_choose": "💸 Choose your withdraw option:",
        "withdraw_low": "You do not have enough stars.",
        "admin_low": "Admin balance is low. Try again later.",
        "withdraw_sent": "✅ Your withdraw request was submitted.",
        "withdraw_failed": "Withdraw request failed. Check ADMIN_ID and channel permissions.",
        "about": "ℹ️ About Us\n\nOur bot is built for Telegram Stars earning. Complete tasks, earn stars, bonuses, and referrals.",
        "support": "📞 Support\n\nPlease message:\n{username}",
        "new_task": "📢 New task added!\n⭐ Reward: {reward}",
        "stats_admin": "👥 Total users: {users}\n🆕 Today users: {today}\n⭐ Total user stars: {stars}\n⭐ Admin stars: {admin_stars}\n📝 Active tasks: {tasks}",
        "admin_only": "This section is admin only.",
        "admin_help": "🛠 Admin Commands\n\n/users\n/refstats\n/withdraws\n/botstats\n/broadcast\n/addtask\n/addbalance\n/taskslist\n/taskstats\n/removetask",
        "broadcast_prompt": "Send the message you want to broadcast.",
        "addtask_link": "Send channel link or @username.",
        "addtask_title": "Send channel title.",
        "addtask_reward": "Send reward, example: 0.5",
        "addbalance_prompt": "Send stars amount to add to admin balance. Example: 1000",
        "addbalance_done": "✅ Admin balance increased by {amount} stars.\n⭐ New balance: {new_balance}",
        "removetask_prompt": "Send Task ID to remove. Example: 5",
        "cancelled": "❌ Action cancelled.",
    },
}


def t(user_id: int, key: str, **kwargs) -> str:
    return TEXTS[get_lang(user_id)][key].format(**kwargs)


# =====================================
# UI
# =====================================
def main_menu(user_id: int):
    keyboard = [
        ["🏧 Withdraw", "⭐ My Stars"],
        ["👥 Referral", "📝 Tasks"],
        ["🎁 Bonus", "🌐 Language"],
        ["ℹ️ About Us", "📞 Support"],
    ]
    if int(user_id) == ADMIN_ID:
        keyboard.insert(0, ["📊 Statistics", "📣 Broadcast"])
        keyboard.insert(1, ["🛠 Add Task", "🗑 Remove Task"])
        keyboard.insert(2, ["➕ Add Balance"])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def cancel_reply_keyboard(user_id: int):
    return ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)


def lang_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🇦🇫 پښتو", callback_data="lang_ps")],
        [InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_main")],
    ])


def force_join_keyboard(user_id: int):
    rows = []
    for username, link in FORCE_JOIN_CHANNELS:
        rows.append([InlineKeyboardButton(f"📢 {username}", url=link)])
    rows.append([InlineKeyboardButton(t(user_id, "joined_btn"), callback_data="check_force_join")])
    return InlineKeyboardMarkup(rows)


def task_keyboard(task_id: int, link: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Open", url=link)],
        [InlineKeyboardButton("✅ Verify", callback_data=f"verify_task_{task_id}")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_main")],
    ])


def withdraw_keyboard():
    rows = []
    for amount in WITHDRAW_OPTIONS:
        rows.append([InlineKeyboardButton(f"⭐ {amount:g} Stars", callback_data=f"withdraw_{amount}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)


def withdraw_admin_keyboard(wd_id: int):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve", callback_data=f"admin_wd_ok_{wd_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"admin_wd_no_{wd_id}"),
        ]
    ])


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
        SELECT ut.id, ut.rewarded_stars, t.chat_username
        FROM user_tasks ut
        JOIN tasks t ON ut.task_id = t.id
        WHERE ut.user_id = %s AND ut.status = 'completed' AND ut.reward_removed = 0
        """,
        (int(user_id),),
    )
    for row in rows:
        if await check_join(bot, row["chat_username"], user_id):
            execute("UPDATE user_tasks SET last_checked_at = %s WHERE id = %s", (now_iso(), row["id"]))
            continue
        add_stars(user_id, -float(row["rewarded_stars"]))
        execute(
            "UPDATE user_tasks SET reward_removed = 1, status = 'left', last_checked_at = %s WHERE id = %s",
            (now_iso(), row["id"]),
        )


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
    ensure_user(int(user.id), user.username or "", user.full_name or "")
    context.user_data.pop("admin_flow", None)

    if context.args:
        arg = context.args[0]
        if arg.startswith("ref_"):
            try:
                referrer_id = int(arg.split("_", 1)[1])
                row = get_user(user.id)
                if row and not row.get("referrer_id") and referrer_id != user.id:
                    execute("UPDATE users SET referrer_id = %s WHERE user_id = %s", (referrer_id, int(user.id)))
            except Exception:
                pass

    row = get_user(user.id)
    if not row or not row.get("lang"):
        await update.message.reply_text(TEXTS["ps"]["choose_lang"], reply_markup=lang_keyboard())
        return

    if not await check_force_join_all(context.bot, user.id):
        await update.message.reply_text(
            t(user.id, "force_join") + "\n\n" + "\n".join(x[0] for x in FORCE_JOIN_CHANNELS),
            reply_markup=force_join_keyboard(user.id),
        )
        return

    await process_leave_penalties(context.bot, user.id)
    await update.message.reply_text(t(user.id, "intro"), reply_markup=main_menu(user.id))


async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    query = update.callback_query
    await query.answer()
    user = query.from_user
    ensure_user(int(user.id), user.username or "", user.full_name or "")
    data = query.data

    if data in ("lang_ps", "lang_en"):
        set_lang(user.id, "ps" if data == "lang_ps" else "en")
        if not await check_force_join_all(context.bot, user.id):
            await query.edit_message_text(
                t(user.id, "force_join") + "\n\n" + "\n".join(x[0] for x in FORCE_JOIN_CHANNELS),
                reply_markup=force_join_keyboard(user.id),
            )
            return
        await query.edit_message_text(t(user.id, "intro"))
        await query.message.reply_text(t(user.id, "intro"), reply_markup=main_menu(user.id))
        return

    if data == "check_force_join":
        if not await check_force_join_all(context.bot, user.id):
            await query.answer(t(user.id, "join_failed"), show_alert=True)
            return
        await query.edit_message_text(t(user.id, "intro"))
        await query.message.reply_text(t(user.id, "intro"), reply_markup=main_menu(user.id))
        return

    if data == "back_main":
        try:
            await query.message.delete()
        except Exception:
            pass
        await query.message.reply_text(t(user.id, "intro"), reply_markup=main_menu(user.id))
        return

    if data.startswith("verify_task_"):
        task_id = int(data.split("_")[-1])
        task = get_task(task_id)
        if not task:
            return
        existing = fetch_one("SELECT * FROM user_tasks WHERE user_id = %s AND task_id = %s", (int(user.id), task_id))
        if existing:
            await query.message.reply_text(t(user.id, "task_already"), reply_markup=main_menu(user.id))
            return
        if not await check_join(context.bot, task["chat_username"], user.id):
            await query.message.reply_text(t(user.id, "task_fail"), reply_markup=main_menu(user.id))
            return

        reward = float(task["reward_stars"])
        if get_stars(ADMIN_ID) < reward:
            await query.message.reply_text(t(user.id, "admin_low"), reply_markup=main_menu(user.id))
            return

        add_stars(ADMIN_ID, -reward)
        add_stars(user.id, reward)
        execute(
            "INSERT INTO user_tasks (user_id, task_id, rewarded_stars, reward_removed, status, created_at, last_checked_at) VALUES (%s, %s, %s, 0, 'completed', %s, %s)",
            (int(user.id), task_id, reward, now_iso(), now_iso()),
        )

        row = get_user(user.id)
if row and row.get("referrer_id"):
    referral_bonus = round((reward * REFERRAL_PERCENT) / 100, 2)
    if referral_bonus > 0:
        add_stars(int(row["referrer_id"]), referral_bonus)

await query.message.reply_text(
    t(user.id, "task_done", stars=f"{reward:g}"),
    reply_markup=main_menu(user.id)
)
return

if data.startswith("withdraw_"):
        amount = float(data.split("_")[-1])
        if get_stars(user.id) < amount:
            await query.message.reply_text(t(user.id, "withdraw_low"), reply_markup=main_menu(user.id))
            return

        add_stars(user.id, -amount)
        
        wd = execute(
         "INSERT INTO withdrawals (user_id, amount, amount_stars, status, created_at) VALUES (%s, %s, %s, %s, %s) RETURNING id",
(int(user.id), amount, amount, "pending", now_iso()),
    
    
         returning=True,
        )
        
        wd_id = wd["id"]
        username = f"@{user.username}" if user.username else (user.full_name or "NoUsername")
        message_text = (
            "📤 New Withdrawal Request!\n\n"
            f"👤 User: {username}\n"
            f"🪪 UserID: {user.id}\n"
            f"💰 Amount: {amount:g} Star ⭐\n"
            f"🕒 Time: {now_pretty()}\n\n"
            "⏳ Status: Pending"
        )

        admin_ok = False
        try:
            await context.bot.send_message(chat_id=ADMIN_ID, text=message_text, reply_markup=withdraw_admin_keyboard(wd_id))
            admin_ok = True
        except Exception as e:
            logger.info("admin withdraw send failed: %s", e)

        channel_message_id = None
        try:
            channel_msg = await context.bot.send_message(chat_id=PAYMENT_CHANNEL, text=message_text)
            channel_message_id = channel_msg.message_id
        except Exception as e:
            logger.info("channel withdraw send failed: %s", e)

        if not admin_ok and not channel_message_id:
            add_stars(user.id, amount)
            execute("UPDATE withdrawals SET status = 'failed' WHERE id = %s", (wd_id,))
            await query.message.reply_text(t(user.id, "withdraw_failed"), reply_markup=main_menu(user.id))
            return

        if channel_message_id:
            execute("UPDATE withdrawals SET channel_message_id = %s WHERE id = %s", (channel_message_id, wd_id))

        await query.message.reply_text(t(user.id, "withdraw_sent"), reply_markup=main_menu(user.id))
        return

        if data.startswith("admin_wd_ok_"):
          if user.id != ADMIN_ID:
            return
        wd_id = int(data.split("_")[-1])
        wd = fetch_one("SELECT * FROM withdrawals WHERE id = %s", (wd_id,))
        if not wd or wd["status"] != "pending":
            return
        execute("UPDATE withdrawals SET status = 'successful', completed_at = %s WHERE id = %s", (now_iso(), wd_id))
        wd_user = get_user(wd["user_id"])
        username = f"@{wd_user['username']}" if wd_user and wd_user.get("username") else (wd_user.get("full_name") if wd_user else "Unknown")
        message_text = (
            "📤 New Withdrawal Request!\n\n"
            f"👤 User: {username}\n"
            f"🪪 UserID: {wd['user_id']}\n"
            f"💰 Amount: {float(wd['amount_stars']):g} Star ⭐\n"
            f"🕒 Time: {now_pretty(wd.get('created_at'))}\n\n"
            "✅ Status: Successful ✅"
        )
        if wd.get("channel_message_id"):
            try:
                await context.bot.edit_message_text(chat_id=PAYMENT_CHANNEL, message_id=wd["channel_message_id"], text=message_text)
            except Exception:
                pass
        try:
            await context.bot.send_message(chat_id=wd["user_id"], text="✅ ستاسو ویډرا بریالی شو.")
        except Exception:
            pass
        return

    if data.startswith("admin_wd_no_"):
        if user.id != ADMIN_ID:
            return
        wd_id = int(data.split("_")[-1])
        wd = fetch_one("SELECT * FROM withdrawals WHERE id = %s", (wd_id,))
        if not wd or wd["status"] != "pending":
            return
        add_stars(wd["user_id"], float(wd["amount_stars"]))
        execute("UPDATE withdrawals SET status = 'rejected', completed_at = %s WHERE id = %s", (now_iso(), wd_id))
        wd_user = get_user(wd["user_id"])
        username = f"@{wd_user['username']}" if wd_user and wd_user.get("username") else (wd_user.get("full_name") if wd_user else "Unknown")
        message_text = (
            "📤 New Withdrawal Request!\n\n"
            f"👤 User: {username}\n"
            f"🪪 UserID: {wd['user_id']}\n"
            f"💰 Amount: {float(wd['amount_stars']):g} Star ⭐\n"
            f"🕒 Time: {now_pretty(wd.get('created_at'))}\n\n"
            "❌ Status: Rejected"
        )
        if wd.get("channel_message_id"):
            try:
                await context.bot.edit_message_text(chat_id=PAYMENT_CHANNEL, message_id=wd["channel_message_id"], text=message_text)
            except Exception:
                pass
        try:
            await context.bot.send_message(chat_id=wd["user_id"], text="❌ ستاسو ویډرا رد شو.")
        except Exception:
            pass
        return


# =====================================
# USER ROUTER
# =====================================
async def user_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    user = update.effective_user
    ensure_user(int(user.id), user.username or "", user.full_name or "")

    if not await check_force_join_all(context.bot, user.id):
        await update.message.reply_text(
            t(user.id, "force_join") + "\n\n" + "\n".join(x[0] for x in FORCE_JOIN_CHANNELS),
            reply_markup=force_join_keyboard(user.id),
        )
        return

    await process_leave_penalties(context.bot, user.id)
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
                users=int(total_users["c"]) if total_users else 0,
                today=int(today_users["c"]) if today_users else 0,
                stars=f"{float(total_stars['s']) if total_stars else 0:g}",
                admin_stars=f"{get_stars(ADMIN_ID):g}",
                tasks=int(active_tasks["c"]) if active_tasks else 0,
            ),
            reply_markup=main_menu(user.id),
        )
        return

    if text == "📣 Broadcast":
        if user.id != ADMIN_ID:
            await update.message.reply_text(t(user.id, "admin_only"), reply_markup=main_menu(user.id))
            return
        context.user_data["admin_flow"] = "broadcast"
        await update.message.reply_text(t(user.id, "broadcast_prompt"), reply_markup=cancel_reply_keyboard(user.id))
        return

    if text == "🛠 Add Task":
        if user.id != ADMIN_ID:
            await update.message.reply_text(t(user.id, "admin_only"), reply_markup=main_menu(user.id))
            return
        context.user_data["admin_flow"] = "addtask_link"
        await update.message.reply_text(t(user.id, "addtask_link"), reply_markup=cancel_reply_keyboard(user.id))
        return

    if text == "🗑 Remove Task":
        if user.id != ADMIN_ID:
            await update.message.reply_text(t(user.id, "admin_only"), reply_markup=main_menu(user.id))
            return
        context.user_data["admin_flow"] = "remove_task"
        await update.message.reply_text(t(user.id, "removetask_prompt"), reply_markup=cancel_reply_keyboard(user.id))
        return

    if text == "➕ Add Balance":
        if user.id != ADMIN_ID:
            await update.message.reply_text(t(user.id, "admin_only"), reply_markup=main_menu(user.id))
            return
        context.user_data["admin_flow"] = "addbalance"
        await update.message.reply_text(t(user.id, "addbalance_prompt"), reply_markup=cancel_reply_keyboard(user.id))
        return

    if text == "🌐 Language":
        await update.message.reply_text(t(user.id, "choose_lang"), reply_markup=lang_keyboard())
        return

    if text == "⭐ My Stars":
        await update.message.reply_text(t(user.id, "my_stars", stars=f"{get_stars(user.id):g}"), reply_markup=main_menu(user.id))
        return

    if text == "👥 Referral":
        await update.message.reply_text(t(user.id, "referral", link=referral_link(user.id), count=referral_count(user.id)), reply_markup=main_menu(user.id))
        return

    if text == "📝 Tasks":
        tasks = fetch_all(
            "SELECT * FROM tasks WHERE status = 'active' AND id NOT IN (SELECT task_id FROM user_tasks WHERE user_id = %s) ORDER BY id DESC",
            (int(user.id),),
        )
        if not tasks:
            await update.message.reply_text(t(user.id, "tasks_empty"), reply_markup=main_menu(user.id))
            return
        task = tasks[0]
        await update.message.reply_text(
            f"📢 {task['channel_title']}\n\n⭐ Reward: {float(task['reward_stars']):g}",
            reply_markup=task_keyboard(task["id"], task["link"]),
        )
        return

    if text == "🎁 Bonus":
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
        await update.message.reply_text(t(user.id, "bonus_added"), reply_markup=main_menu(user.id))
        return

    if text == "🏧 Withdraw":
        await update.message.reply_text(t(user.id, "withdraw_choose"), reply_markup=withdraw_keyboard())
        return

    if text == "ℹ️ About Us":
        await update.message.reply_text(t(user.id, "about"), reply_markup=main_menu(user.id))
        return

    if text == "📞 Support":
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
        lines.append(f"{r['user_id']} | @{r['username'] or 'no_username'} | ⭐ {float(r['stars'] or 0):g} | ref_by: {ref_by}")
    await update.message.reply_text("\n".join(lines) or "No users")

def get_user_refs(user_id):
    return fetch_all(
        "SELECT user_id AS id, username FROM users WHERE referrer_id = %s",
        (user_id,)
    )
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

        lines.append(f"{i}. {username} - {len(refs)} refs\n👉 {ref_list}")

    await update.message.reply_text("🏆 Referral Leaderboard\n\n" + "\n\n".join(lines))

async def admin_withdraws(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return
    rows = fetch_all("SELECT * FROM withdrawals WHERE status = 'pending' ORDER BY created_at DESC LIMIT 50")
    text = "\n".join([f"#{r['id']} | User {r['user_id']} | ⭐ {float(r['amount_stars']):g} | {r['status']}" for r in rows]) or "No pending withdraws"
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
        "📊 Bot Stats\n\n"
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
    text = "\n".join([f"#{r['id']} | {r['channel_title']} | ⭐ {float(r['reward_stars']):g} | {r['status']}" for r in rows]) or "No tasks"
    await update.message.reply_text(text)


async def admin_taskstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return
    rows = fetch_all(
        """
        SELECT t.id, t.channel_title, t.status, COUNT(ut.id) AS joined_count
        FROM tasks t
        LEFT JOIN user_tasks ut ON ut.task_id = t.id AND ut.reward_removed = 0
        GROUP BY t.id, t.channel_title, t.status
        ORDER BY t.id DESC
        LIMIT 100
        """
    )
    text = "\n".join([f"#{r['id']} | {r['channel_title']} | joins: {r['joined_count']} | {r['status']}" for r in rows]) or "No task stats"
    await update.message.reply_text(text)


async def admin_removetask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return
    if not context.args:
        await update.message.reply_text("Usage: /removetask TASK_ID")
        return
    try:
        task_id = int(context.args[0])
    except Exception:
        await update.message.reply_text("Invalid task id")
        return
    execute("UPDATE tasks SET status = 'removed' WHERE id = %s", (task_id,))
    await update.message.reply_text(f"✅ Task #{task_id} removed")


async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return
    context.user_data["admin_flow"] = "broadcast"
    await update.message.reply_text(t(update.effective_user.id, "broadcast_prompt"), reply_markup=cancel_reply_keyboard(update.effective_user.id))


async def admin_addtask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return
    context.user_data["admin_flow"] = "addtask_link"
    await update.message.reply_text(t(update.effective_user.id, "addtask_link"), reply_markup=cancel_reply_keyboard(update.effective_user.id))


async def admin_addbalance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(t(update.effective_user.id, "admin_only"))
        return
    context.user_data["admin_flow"] = "addbalance"
    await update.message.reply_text(t(update.effective_user.id, "addbalance_prompt"), reply_markup=cancel_reply_keyboard(update.effective_user.id))


async def admin_flow_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not is_private(update) or update.effective_user.id != ADMIN_ID:
        return False

    flow = context.user_data.get("admin_flow")
    if not flow:
        return False

    text = (update.message.text or "").strip()

    if text.lower() in ("cancel", "/cancel", "❌ cancel", "back", "⬅️ back"):
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
        await update.message.reply_text(f"✅ Sent: {sent}\n❌ Failed: {failed}", reply_markup=main_menu(update.effective_user.id))
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
        for u in fetch_all("SELECT user_id FROM users"):
            try:
                await context.bot.send_message(chat_id=u["user_id"], text=t(u["user_id"], "new_task", reward=f"{reward:g}"))
            except Exception:
                pass
        context.user_data.pop("admin_flow", None)
        await update.message.reply_text("✅ Task added", reply_markup=main_menu(update.effective_user.id))
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

    if flow == "remove_task":
        try:
            task_id = int(text)
        except Exception:
            await update.message.reply_text("Invalid task id. Example: 5", reply_markup=cancel_reply_keyboard(update.effective_user.id))
            return True
        execute("UPDATE tasks SET status = 'removed' WHERE id = %s", (task_id,))
        context.user_data.pop("admin_flow", None)
        await update.message.reply_text(f"✅ Task #{task_id} removed", reply_markup=main_menu(update.effective_user.id))
        return True

    return False


# =====================================
# MAIN
# =====================================
def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(buttons))
    app.add_handler(ChatMemberHandler(track_bot_chats, ChatMemberHandler.MY_CHAT_MEMBER))

    app.add_handler(CommandHandler("admin", admin_help))
    app.add_handler(CommandHandler("users", admin_users))
    app.add_handler(CommandHandler("refstats", admin_refstats))
    app.add_handler(CommandHandler("withdraws", admin_withdraws))
    app.add_handler(CommandHandler("botstats", admin_botstats))
    app.add_handler(CommandHandler("taskslist", admin_taskslist))
    app.add_handler(CommandHandler("taskstats", admin_taskstats))
    app.add_handler(CommandHandler("removetask", admin_removetask))
    app.add_handler(CommandHandler("broadcast", admin_broadcast))
    app.add_handler(CommandHandler("addtask", admin_addtask))
    app.add_handler(CommandHandler("addbalance", admin_addbalance))

    async def combined_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
        handled = await admin_flow_router(update, context)
        if handled:
            return
        await user_router(update, context)

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, combined_router))

    if app.job_queue:
        app.job_queue.run_repeating(periodic_leave_check, interval=LEAVE_CHECK_INTERVAL_HOURS * 3600, first=600)
        app.job_queue.run_repeating(daily_promo_post, interval=PROMO_INTERVAL_HOURS * 3600, first=900)

    logger.info("EasyEarn stars final fixed code is running...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
