import sqlite3
import logging
from datetime import datetime, timedelta, timezone

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# =========================
# CONFIG
# =========================
BOT_TOKEN = "8713775500:AAE1XxzR3T6BKp22HmsCc9NU7cZg-htE6Bc"
ADMIN_ID = 1347546821
BOT_USERNAME = "EasyEarnAppBot"

FORCE_JOIN_USERNAME = "@easyearnofficial1222"
FORCE_JOIN_LINK = "https://t.me/easyearnofficial1222"

HESAB_PAY = "+93708310201"
ATOMA_PAY = "+93770876916"

DB_NAME = "easyearn.db"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =========================
# DATABASE
# =========================
conn = sqlite3.connect(DB_NAME, check_same_thread=False)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    full_name TEXT,
    lang TEXT DEFAULT 'ps',
    role TEXT,
    balance INTEGER DEFAULT 0,
    referrer_id INTEGER,
    referral_paid INTEGER DEFAULT 0,
    last_bonus_at TEXT,
    created_at TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS campaigns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_user_id INTEGER NOT NULL,
    title_ps TEXT NOT NULL,
    title_en TEXT NOT NULL,
    reward_afn INTEGER NOT NULL,
    target_type TEXT NOT NULL,   -- channel/group
    link TEXT NOT NULL,
    chat_username TEXT NOT NULL,
    status TEXT DEFAULT 'active',
    completed_count INTEGER DEFAULT 0,
    created_at TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS user_campaigns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    campaign_id INTEGER NOT NULL,
    status TEXT DEFAULT 'completed',
    created_at TEXT,
    UNIQUE(user_id, campaign_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS deposits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    method TEXT NOT NULL,
    proof_file_id TEXT,
    status TEXT DEFAULT 'pending',
    created_at TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS withdrawals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    network TEXT NOT NULL,
    phone TEXT NOT NULL,
    full_name TEXT NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TEXT
)
""")

conn.commit()

# =========================
# HELPERS
# =========================
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def ensure_user(user_id: int, username: str, full_name: str):
    cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    if not row:
        cur.execute("""
            INSERT INTO users (user_id, username, full_name, created_at)
            VALUES (?, ?, ?, ?)
        """, (user_id, username, full_name, now_iso()))
    else:
        cur.execute("""
            UPDATE users
            SET username = ?, full_name = ?
            WHERE user_id = ?
        """, (username, full_name, user_id))
    conn.commit()

def ensure_admin_account():
    cur.execute("SELECT * FROM users WHERE user_id = ?", (ADMIN_ID,))
    row = cur.fetchone()
    if not row:
        cur.execute("""
            INSERT INTO users (user_id, username, full_name, lang, role, balance, created_at)
            VALUES (?, ?, ?, 'ps', 'client', 0, ?)
        """, (ADMIN_ID, "admin", "Admin", now_iso()))
        conn.commit()

def get_user(user_id: int):
    cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    return cur.fetchone()

def set_lang(user_id: int, lang: str):
    cur.execute("UPDATE users SET lang = ? WHERE user_id = ?", (lang, user_id))
    conn.commit()

def user_lang(user_id: int) -> str:
    row = get_user(user_id)
    return row["lang"] if row and row["lang"] else "ps"

def set_role(user_id: int, role: str):
    cur.execute("UPDATE users SET role = ? WHERE user_id = ?", (role, user_id))
    conn.commit()

def user_role(user_id: int):
    row = get_user(user_id)
    return row["role"] if row else None

def get_balance(user_id: int) -> int:
    row = get_user(user_id)
    return int(row["balance"]) if row else 0

def change_balance(user_id: int, amount: int):
    cur.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
    conn.commit()

def referral_link(user_id: int) -> str:
    return f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"

def referral_count(user_id: int) -> int:
    cur.execute("SELECT COUNT(*) AS c FROM users WHERE referrer_id = ?", (user_id,))
    return cur.fetchone()["c"]

def get_campaign(campaign_id: int):
    cur.execute("SELECT * FROM campaigns WHERE id = ?", (campaign_id,))
    return cur.fetchone()

def seed_demo_campaigns():
    cur.execute("SELECT COUNT(*) AS c FROM campaigns")
    if cur.fetchone()["c"] > 0:
        return

    demo_campaigns = [
        {
            "title_ps": "Sharifi Graphix چینل جوین کړئ",
            "title_en": "Join Sharifi Graphix channel",
            "reward_afn": 2,
            "target_type": "channel",
            "link": "https://t.me/designskills211",
            "chat_username": "@designskills211",
        },
        {
            "title_ps": "Khan Digital Group چینل جوین کړئ",
            "title_en": "Join Khan Digital Group channel",
            "reward_afn": 2,
            "target_type": "channel",
            "link": "https://t.me/haqyarserviceso1",
            "chat_username": "@haqyarserviceso1",
        },
        {
            "title_ps": "Example Group جوین کړئ",
            "title_en": "Join Example Group",
            "reward_afn": 1,
            "target_type": "group",
            "link": "https://t.me/examplegroup",
            "chat_username": "@examplegroup",
        },
    ]

    for c in demo_campaigns:
        cur.execute("""
            INSERT INTO campaigns (
                owner_user_id, title_ps, title_en, reward_afn,
                target_type, link, chat_username, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ADMIN_ID,
            c["title_ps"],
            c["title_en"],
            c["reward_afn"],
            c["target_type"],
            c["link"],
            c["chat_username"],
            now_iso()
        ))
    conn.commit()

def deposit_fee(amount: int):
    final_amount = int(amount * 0.9)
    fee_amount = amount - final_amount
    return final_amount, fee_amount

def withdraw_fee(amount: int):
    final_amount = int(amount * 0.9)
    fee_amount = amount - final_amount
    return final_amount, fee_amount

ensure_admin_account()
seed_demo_campaigns()

# =========================
# TEXTS
# =========================
TEXTS = {
    "ps": {
        "choose_lang": "مهرباني وکړئ ژبه انتخاب کړئ:",
        "must_join": "🚀 د بوټ کارولو لپاره مهرباني وکړئ اول زمونږ چینل جوین کړئ:",
        "joined_btn": "✅ جوین مې کړ",
        "join_failed": "❌ مهرباني وکړئ اول چینل جوین کړئ.",
        "choose_role": "مهرباني وکړئ خپل رول انتخاب کړئ:",
        "worker": "👷 کارکوونکي",
        "client": "📢 چینل لرونکي",
        "welcome_worker": "👋 ښه راغلاست کارکونکي ته",
        "welcome_client": "👋 ښه راغلاست چینل لرونکي ته",
        "main_menu": "لاندې یو انتخاب وکړئ:",
        "balance": "💰 ستاسو بیلانس: {amount} افغانۍ",
        "bonus_added": "🎁 1 AFN ستاسو بیلانس ته اضافه شوه.",
        "bonus_wait": "⏳ تاسو نن ورځنی بونس اخیستی.",
        "bonus_admin_low": "❌ د بونس ورکولو لپاره د اډمین بیلانس کم دی.",
        "referral": "👥 ستاسو د ریفرل لینک:\n{link}\n\nټول ریفرلونه: {count}",
        "tasks_empty": "هیڅ task نشته.",
        "open": "🔗 خلاص کړه",
        "verify": "✅ وریفای",
        "back": "⬅️ شاته",
        "task_done": "🎉 مبارک! {reward} AFN ستاسو بیلانس ته اضافه شول.",
        "task_already": "✅ تاسو دا task مخکې بشپړ کړی.",
        "task_fail": "❌ مهرباني وکړئ اول task بشپړ کړئ، بیا وریفای وکړئ.",
        "task_owner_low": "❌ د کمپاین د مالک بیلانس کم دی.",
        "deposit_menu": "➕ ډیپازټ\n\nلږ تر لږه 100 AFN\n10% فیس به حسابیږي\n\nMethod انتخاب کړئ:",
        "deposit_amount_prompt": "هغه اندازه ولیکئ چې لیږلې مو ده:",
        "deposit_proof_prompt": "اوس screenshot ولیږئ.",
        "deposit_sent": "✅ ستاسو د ډیپازټ غوښتنه د تایید لپاره ولېږل شوه.",
        "withdraw_min": "❌ د ویډرا لږ تر لږه اندازه 50 AFN ده.",
        "withdraw_low_balance": "❌ ستاسو بیلانس کم دی.",
        "withdraw_ask_amount": "💸 د ویډرا اندازه ولیکئ:",
        "withdraw_ask_name": "خپل بشپړ نوم ولیکئ:",
        "withdraw_ask_phone": "خپله شمېره ولیکئ:",
        "withdraw_ask_network": "خپله شبکه ولیکئ:\nAfghan Wireless / Etisalat / Roshan / Salaam / Atoma",
        "withdraw_sent": "✅ ستاسو د ویډرا غوښتنه د تایید لپاره ولېږل شوه.",
        "campaign_created": "✅ کمپاین جوړ شو.",
        "campaigns_empty": "تاسو لا کمپاین نه لرئ.",
        "ask_link": "مهرباني وکړئ د چینل یا ګروپ لینک راولېږئ:",
        "ask_username": "مهرباني وکړئ username راولېږئ، مثال: @channelname",
        "approved_deposit": "✅ ستاسو ډیپازټ تایید شو.",
        "approved_withdraw": "✅ ستاسو ویډرا تایید شوه.",
        "rejected": "❌ ستاسو غوښتنه رد شوه.",
    },
    "en": {
        "choose_lang": "Please choose your language:",
        "must_join": "🚀 To use the bot, please join our channel first:",
        "joined_btn": "✅ I Joined",
        "join_failed": "❌ Please join the channel first.",
        "choose_role": "Please choose your role:",
        "worker": "👷 Worker",
        "client": "📢 Client",
        "welcome_worker": "👋 Welcome Worker",
        "welcome_client": "👋 Welcome Client",
        "main_menu": "Choose an option below:",
        "balance": "💰 Your Balance: {amount} AFN",
        "bonus_added": "🎁 1 AFN has been added to your balance.",
        "bonus_wait": "⏳ You already claimed your daily bonus.",
        "bonus_admin_low": "❌ Admin balance is too low for bonus.",
        "referral": "👥 Your referral link:\n{link}\n\nTotal referrals: {count}",
        "tasks_empty": "No tasks found.",
        "open": "🔗 Open",
        "verify": "✅ Verify",
        "back": "⬅️ Back",
        "task_done": "🎉 Congratulations! {reward} AFN has been added to your balance.",
        "task_already": "✅ You already completed this task.",
        "task_fail": "❌ Please complete the task first, then verify.",
        "task_owner_low": "❌ Campaign owner balance is low.",
        "deposit_menu": "➕ Deposit\n\nMinimum 100 AFN\n10% fee applies\n\nChoose method:",
        "deposit_amount_prompt": "Enter the amount you sent:",
        "deposit_proof_prompt": "Now send screenshot.",
        "deposit_sent": "✅ Your deposit request has been sent for approval.",
        "withdraw_min": "❌ Minimum withdraw is 50 AFN.",
        "withdraw_low_balance": "❌ Insufficient balance.",
        "withdraw_ask_amount": "💸 Enter withdraw amount:",
        "withdraw_ask_name": "Enter your full name:",
        "withdraw_ask_phone": "Enter your phone number:",
        "withdraw_ask_network": "Enter your network:\nAfghan Wireless / Etisalat / Roshan / Salaam / Atoma",
        "withdraw_sent": "✅ Your withdraw request has been sent for approval.",
        "campaign_created": "✅ Campaign created.",
        "campaigns_empty": "You have no campaigns yet.",
        "ask_link": "Please send channel or group link:",
        "ask_username": "Please send username, example: @channelname",
        "approved_deposit": "✅ Your deposit was approved.",
        "approved_withdraw": "✅ Your withdraw was approved.",
        "rejected": "❌ Your request was rejected.",
    }
}

def t(user_id: int, key: str, **kwargs):
    return TEXTS[user_lang(user_id)][key].format(**kwargs)

# =========================
# KEYBOARDS
# =========================
def lang_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🇦🇫 پښتو", callback_data="lang_ps"),
            InlineKeyboardButton("🇬🇧 English", callback_data="lang_en"),
        ]
    ])

def force_join_keyboard(user_id: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Join Channel", url=FORCE_JOIN_LINK)],
        [InlineKeyboardButton(t(user_id, "joined_btn"), callback_data="check_force_join")],
    ])

def role_keyboard(user_id: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(t(user_id, "worker"), callback_data="role_worker")],
        [InlineKeyboardButton(t(user_id, "client"), callback_data="role_client")],
    ])

def worker_menu(user_id: int):
    lang = user_lang(user_id)
    if lang == "ps":
        rows = [
            [InlineKeyboardButton("💰 زما بیلانس", callback_data="worker_balance"),
             InlineKeyboardButton("📢 دندې", callback_data="worker_tasks")],
            [InlineKeyboardButton("🎁 ورځنی بونس", callback_data="worker_bonus"),
             InlineKeyboardButton("👥 ریفرل", callback_data="worker_referral")],
            [InlineKeyboardButton("💸 ویډرا", callback_data="worker_withdraw"),
             InlineKeyboardButton("🌐 ژبه بدلول", callback_data="change_lang")],
        ]
    else:
        rows = [
            [InlineKeyboardButton("💰 My Balance", callback_data="worker_balance"),
             InlineKeyboardButton("📢 Tasks", callback_data="worker_tasks")],
            [InlineKeyboardButton("🎁 Daily Bonus", callback_data="worker_bonus"),
             InlineKeyboardButton("👥 Referral", callback_data="worker_referral")],
            [InlineKeyboardButton("💸 Withdraw", callback_data="worker_withdraw"),
             InlineKeyboardButton("🌐 Change Language", callback_data="change_lang")],
        ]
    return InlineKeyboardMarkup(rows)

def client_menu(user_id: int):
    lang = user_lang(user_id)
    if lang == "ps":
        rows = [
            [InlineKeyboardButton("💰 زما بیلانس", callback_data="client_balance"),
             InlineKeyboardButton("💳 ډیپازټ", callback_data="client_deposit")],
            [InlineKeyboardButton("➕ چینل ټاسک", callback_data="client_add_channel"),
             InlineKeyboardButton("➕ ګروپ ټاسک", callback_data="client_add_group")],
            [InlineKeyboardButton("📊 زما کمپاینونه", callback_data="client_campaigns"),
             InlineKeyboardButton("🎁 ورځنی بونس", callback_data="client_bonus")],
            [InlineKeyboardButton("👥 ریفرل", callback_data="client_referral"),
             InlineKeyboardButton("🌐 ژبه بدلول", callback_data="change_lang")],
        ]
    else:
        rows = [
            [InlineKeyboardButton("💰 My Balance", callback_data="client_balance"),
             InlineKeyboardButton("💳 Deposit", callback_data="client_deposit")],
            [InlineKeyboardButton("➕ Add Channel Task", callback_data="client_add_channel"),
             InlineKeyboardButton("➕ Add Group Task", callback_data="client_add_group")],
            [InlineKeyboardButton("📊 My Campaigns", callback_data="client_campaigns"),
             InlineKeyboardButton("🎁 Daily Bonus", callback_data="client_bonus")],
            [InlineKeyboardButton("👥 Referral", callback_data="client_referral"),
             InlineKeyboardButton("🌐 Change Language", callback_data="change_lang")],
        ]
    return InlineKeyboardMarkup(rows)

def campaign_list_keyboard(user_id: int, campaigns):
    rows = []
    lang = user_lang(user_id)
    for c in campaigns:
        title = c["title_ps"] if lang == "ps" else c["title_en"]
        rows.append([InlineKeyboardButton(title, callback_data=f"open_campaign_{c['id']}")])
    rows.append([InlineKeyboardButton(t(user_id, "back"), callback_data="back_worker_menu")])
    return InlineKeyboardMarkup(rows)

def task_card_keyboard(user_id: int, campaign_id: int, link: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(t(user_id, "open"), url=link)],
        [InlineKeyboardButton(t(user_id, "verify"), callback_data=f"verify_campaign_{campaign_id}")],
        [InlineKeyboardButton(t(user_id, "back"), callback_data="worker_tasks")],
    ])

# =========================
# VERIFY
# =========================
async def check_join(bot, chat_username: str, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=chat_username, user_id=user_id)
        return member.status in ("member", "administrator", "creator")
    except Exception as e:
        logger.error(f"Join check failed: {e}")
        return False

# =========================
# START
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.username or "", user.full_name or "")

    # referral save
    if context.args:
        arg = context.args[0]
        if arg.startswith("ref_"):
            try:
                referrer_id = int(arg.split("_")[1])
                row = get_user(user.id)
                if referrer_id != user.id and row and row["referrer_id"] is None:
                    cur.execute("UPDATE users SET referrer_id = ? WHERE user_id = ?", (referrer_id, user.id))
                    conn.commit()
            except Exception:
                pass

    row = get_user(user.id)

    # check force join
    joined = await check_join(context.bot, FORCE_JOIN_USERNAME, user.id)
    if not joined:
        await update.message.reply_text(
            t(user.id, "must_join"),
            reply_markup=force_join_keyboard(user.id)
        )
        return

    # go to menu if role exists
    if row and row["role"] == "worker":
        await update.message.reply_text(
            f"{t(user.id, 'welcome_worker')}\n\n{t(user.id, 'main_menu')}",
            reply_markup=worker_menu(user.id)
        )
        return

    if row and row["role"] == "client":
        await update.message.reply_text(
            f"{t(user.id, 'welcome_client')}\n\n{t(user.id, 'main_menu')}",
            reply_markup=client_menu(user.id)
        )
        return

    # otherwise language selection
    await update.message.reply_text(
        TEXTS["ps"]["choose_lang"],
        reply_markup=lang_keyboard()
    )
# =========================
# CALLBACKS
# =========================
async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user = update.effective_user
    ensure_user(user.id, user.username or "", user.full_name or "")
    data = query.data

    # language
    if data == "lang_ps":
    set_lang(user.id, "ps")
    await query.edit_message_text("PS OK")
    return

if data == "lang_en":
    set_lang(user.id, "en")
    await query.edit_message_text("EN OK")
    return
    
    if data == "change_lang":
        await query.edit_message_text(TEXTS["ps"]["choose_lang"], reply_markup=lang_keyboard())
        return

    # force join
    if data == "check_force_join":
        joined = await check_join(context.bot, FORCE_JOIN_USERNAME, user.id)
        if not joined:
            await query.answer(t(user.id, "join_failed"), show_alert=True)
            return
        await query.edit_message_text(t(user.id, "choose_role"), reply_markup=role_keyboard(user.id))
        return

    # roles
    if data == "role_worker":
        set_role(user.id, "worker")
        await query.edit_message_text(
            f"{t(user.id, 'welcome_worker')}\n\n{t(user.id, 'main_menu')}",
            reply_markup=worker_menu(user.id)
        )
        return

    if data == "role_client":
        set_role(user.id, "client")
        await query.edit_message_text(
            f"{t(user.id, 'welcome_client')}\n\n{t(user.id, 'main_menu')}",
            reply_markup=client_menu(user.id)
        )
        return

    # worker menu
    if data == "worker_balance":
        await query.edit_message_text(
            t(user.id, "balance", amount=get_balance(user.id)),
            reply_markup=worker_menu(user.id)
        )
        return

    if data == "worker_bonus":
        if get_balance(ADMIN_ID) < 1:
            await query.edit_message_text(t(user.id, "bonus_admin_low"), reply_markup=worker_menu(user.id))
            return

        row = get_user(user.id)
        if row["last_bonus_at"]:
            last = datetime.fromisoformat(row["last_bonus_at"])
            if datetime.now(timezone.utc) - last < timedelta(hours=24):
                await query.edit_message_text(t(user.id, "bonus_wait"), reply_markup=worker_menu(user.id))
                return

        change_balance(ADMIN_ID, -1)
        change_balance(user.id, 1)
        cur.execute("UPDATE users SET last_bonus_at = ? WHERE user_id = ?", (now_iso(), user.id))
        conn.commit()

        await query.edit_message_text(t(user.id, "bonus_added"), reply_markup=worker_menu(user.id))
        return

    if data == "worker_referral":
        await query.edit_message_text(
            t(user.id, "referral", link=referral_link(user.id), count=referral_count(user.id)),
            reply_markup=worker_menu(user.id)
        )
        return

    if data == "worker_tasks":
        cur.execute("SELECT * FROM campaigns WHERE status = 'active' ORDER BY id DESC")
        campaigns = cur.fetchall()
        if not campaigns:
            await query.edit_message_text(t(user.id, "tasks_empty"), reply_markup=worker_menu(user.id))
            return
        await query.edit_message_text(t(user.id, "tasks_empty").replace("هیڅ task نشته.", "📢 موجود ټاسکونه").replace("No tasks found.", "📢 Available Tasks"), reply_markup=campaign_list_keyboard(user.id, campaigns))
        return

    if data == "back_worker_menu":
        await query.edit_message_text(
            f"{t(user.id, 'welcome_worker')}\n\n{t(user.id, 'main_menu')}",
            reply_markup=worker_menu(user.id)
        )
        return

    if data.startswith("open_campaign_"):
        campaign_id = int(data.split("_")[-1])
        c = get_campaign(campaign_id)
        if not c:
            return

        title = c["title_ps"] if user_lang(user.id) == "ps" else c["title_en"]
        text = f"{title}\n\n💰 Reward: {c['reward_afn']} AFN"
        await query.edit_message_text(text, reply_markup=task_card_keyboard(user.id, c["id"], c["link"]))
        return

    if data.startswith("verify_campaign_"):
        campaign_id = int(data.split("_")[-1])
        c = get_campaign(campaign_id)
        if not c:
            return

        cur.execute("SELECT * FROM user_campaigns WHERE user_id = ? AND campaign_id = ?", (user.id, campaign_id))
        existing = cur.fetchone()
        if existing:
            await query.edit_message_text(t(user.id, "task_already"), reply_markup=worker_menu(user.id))
            return

        ok = await check_join(context.bot, c["chat_username"], user.id)
        if not ok:
            await query.edit_message_text(t(user.id, "task_fail"), reply_markup=worker_menu(user.id))
            return

        owner_id = c["owner_user_id"]
        reward = c["reward_afn"]

        if get_balance(owner_id) < reward:
            await query.edit_message_text(t(user.id, "task_owner_low"), reply_markup=worker_menu(user.id))
            return

        # task reward
        change_balance(owner_id, -reward)
        change_balance(user.id, reward)

        # referral reward only once, from admin balance
        worker_row = get_user(user.id)
        if worker_row and worker_row["referrer_id"] and worker_row["referral_paid"] == 0:
            if get_balance(ADMIN_ID) >= 2:
                change_balance(ADMIN_ID, -2)
                change_balance(worker_row["referrer_id"], 2)
                cur.execute("UPDATE users SET referral_paid = 1 WHERE user_id = ?", (user.id,))
                conn.commit()

        cur.execute("""
            INSERT INTO user_campaigns (user_id, campaign_id, status, created_at)
            VALUES (?, ?, 'completed', ?)
        """, (user.id, campaign_id, now_iso()))
        cur.execute("UPDATE campaigns SET completed_count = completed_count + 1 WHERE id = ?", (campaign_id,))
        conn.commit()

        await query.edit_message_text(t(user.id, "task_done", reward=reward), reply_markup=worker_menu(user.id))
        return

    if data == "worker_withdraw":
        context.user_data["flow"] = "withdraw"
        context.user_data["withdraw_step"] = "amount"
        await query.message.reply_text(t(user.id, "withdraw_ask_amount"))
        return

    # client menu
    if data == "client_balance":
        await query.edit_message_text(
            t(user.id, "balance", amount=get_balance(user.id)),
            reply_markup=client_menu(user.id)
        )
        return

    if data == "client_bonus":
        if get_balance(ADMIN_ID) < 1:
            await query.edit_message_text(t(user.id, "bonus_admin_low"), reply_markup=client_menu(user.id))
            return

        row = get_user(user.id)
        if row["last_bonus_at"]:
            last = datetime.fromisoformat(row["last_bonus_at"])
            if datetime.now(timezone.utc) - last < timedelta(hours=24):
                await query.edit_message_text(t(user.id, "bonus_wait"), reply_markup=client_menu(user.id))
                return

        change_balance(ADMIN_ID, -1)
        change_balance(user.id, 1)
        cur.execute("UPDATE users SET last_bonus_at = ? WHERE user_id = ?", (now_iso(), user.id))
        conn.commit()

        await query.edit_message_text(t(user.id, "bonus_added"), reply_markup=client_menu(user.id))
        return

    if data == "client_referral":
        await query.edit_message_text(
            t(user.id, "referral", link=referral_link(user.id), count=referral_count(user.id)),
            reply_markup=client_menu(user.id)
        )
        return

    if data == "client_deposit":
        context.user_data["flow"] = "deposit"
        context.user_data["deposit_step"] = "method"

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Hesab Pay", callback_data="deposit_method_hesab")],
            [InlineKeyboardButton("Atoma Pay", callback_data="deposit_method_atoma")],
        ])
        await query.edit_message_text(t(user.id, "deposit_menu"), reply_markup=keyboard)
        return

    if data.startswith("deposit_method_"):
        method = data.split("_")[-1]
        context.user_data["deposit_method"] = method
        context.user_data["deposit_step"] = "amount"

        number = HESAB_PAY if method == "hesab" else ATOMA_PAY
        await query.message.reply_text(
            f"Send to: {number}\n\n{t(user.id, 'deposit_amount_prompt')}"
        )
        return

    if data == "client_add_channel":
        context.user_data["flow"] = "create_campaign"
        context.user_data["campaign_type"] = "channel"
        context.user_data["campaign_step"] = "link"
        await query.message.reply_text(t(user.id, "ask_link"))
        return

    if data == "client_add_group":
        context.user_data["flow"] = "create_campaign"
        context.user_data["campaign_type"] = "group"
        context.user_data["campaign_step"] = "link"
        await query.message.reply_text(t(user.id, "ask_link"))
        return

    if data == "client_campaigns":
        cur.execute("SELECT * FROM campaigns WHERE owner_user_id = ? ORDER BY id DESC", (user.id,))
        rows = cur.fetchall()
        if not rows:
            await query.edit_message_text(t(user.id, "campaigns_empty"), reply_markup=client_menu(user.id))
            return

        lines = []
        for r in rows:
            title = r["title_ps"] if user_lang(user.id) == "ps" else r["title_en"]
            lines.append(f"• {title}\nReward: {r['reward_afn']} AFN | Done: {r['completed_count']}")
        await query.edit_message_text("\n\n".join(lines), reply_markup=client_menu(user.id))
        return

    # admin deposit approve/reject
    if data.startswith("admin_approve_deposit_"):
        if user.id != ADMIN_ID:
            return
        dep_id = int(data.split("_")[-1])
        cur.execute("SELECT * FROM deposits WHERE id = ?", (dep_id,))
        dep = cur.fetchone()
        if not dep or dep["status"] != "pending":
            return

        final_amount, fee_amount = deposit_fee(dep["amount"])
        change_balance(dep["user_id"], final_amount)
        cur.execute("UPDATE deposits SET status = 'approved' WHERE id = ?", (dep_id,))
        conn.commit()

        await context.bot.send_message(
            dep["user_id"],
            f"{t(dep['user_id'], 'approved_deposit')}\n\nRequested: {dep['amount']} AFN\nFee: {fee_amount} AFN\nAdded: {final_amount} AFN"
        )
        await query.edit_message_caption(caption=(query.message.caption or "") + f"\n\n✅ Approved\nFee: {fee_amount} AFN\nAdded: {final_amount} AFN")
        return

    if data.startswith("admin_reject_deposit_"):
        if user.id != ADMIN_ID:
            return
        dep_id = int(data.split("_")[-1])
        cur.execute("SELECT * FROM deposits WHERE id = ?", (dep_id,))
        dep = cur.fetchone()
        if not dep or dep["status"] != "pending":
            return

        cur.execute("UPDATE deposits SET status = 'rejected' WHERE id = ?", (dep_id,))
        conn.commit()

        await context.bot.send_message(dep["user_id"], t(dep["user_id"], "rejected"))
        await query.edit_message_caption(caption=(query.message.caption or "") + "\n\n❌ Rejected")
        return

    # admin withdraw approve/reject
    if data.startswith("admin_approve_withdraw_"):
        if user.id != ADMIN_ID:
            return
        wd_id = int(data.split("_")[-1])
        cur.execute("SELECT * FROM withdrawals WHERE id = ?", (wd_id,))
        wd = cur.fetchone()
        if not wd or wd["status"] != "pending":
            return

        final_amount, fee_amount = withdraw_fee(wd["amount"])
        cur.execute("UPDATE withdrawals SET status = 'approved' WHERE id = ?", (wd_id,))
        conn.commit()

        await context.bot.send_message(
            wd["user_id"],
            f"{t(wd['user_id'], 'approved_withdraw')}\n\nRequested: {wd['amount']} AFN\nFee: {fee_amount} AFN\nYou will receive: {final_amount} AFN"
        )
        await query.edit_message_text(query.message.text + f"\n\n✅ Approved\nFee: {fee_amount} AFN\nSend: {final_amount} AFN")
        return

    if data.startswith("admin_reject_withdraw_"):
        if user.id != ADMIN_ID:
            return
        wd_id = int(data.split("_")[-1])
        cur.execute("SELECT * FROM withdrawals WHERE id = ?", (wd_id,))
        wd = cur.fetchone()
        if not wd or wd["status"] != "pending":
            return

        change_balance(wd["user_id"], wd["amount"])
        cur.execute("UPDATE withdrawals SET status = 'rejected' WHERE id = ?", (wd_id,))
        conn.commit()

        await context.bot.send_message(wd["user_id"], t(wd["user_id"], "rejected"))
        await query.edit_message_text(query.message.text + "\n\n❌ Rejected + Refunded")
        return

# =========================
# TEXT / PHOTO FLOW
# =========================
async def flow_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.username or "", user.full_name or "")
    flow = context.user_data.get("flow")

    if not flow:
        return

    # deposit
    if flow == "deposit":
        step = context.user_data.get("deposit_step")

        if step == "amount":
            try:
                amount = int(update.message.text.strip())
                if amount < 100:
                    await update.message.reply_text("Minimum deposit is 100 AFN.")
                    return
            except Exception:
                await update.message.reply_text("Enter a valid amount.")
                return

            context.user_data["deposit_amount"] = amount
            context.user_data["deposit_step"] = "proof"
            await update.message.reply_text(t(user.id, "deposit_proof_prompt"))
            return

        if step == "proof":
            if not update.message.photo:
                await update.message.reply_text(t(user.id, "deposit_proof_prompt"))
                return

            amount = context.user_data["deposit_amount"]
            method = context.user_data["deposit_method"]
            proof_file_id = update.message.photo[-1].file_id

            cur.execute("""
                INSERT INTO deposits (user_id, amount, method, proof_file_id, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (user.id, amount, method, proof_file_id, now_iso()))
            conn.commit()
            dep_id = cur.lastrowid

            final_amount, fee_amount = deposit_fee(amount)

            buttons = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Approve", callback_data=f"admin_approve_deposit_{dep_id}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"admin_reject_deposit_{dep_id}"),
            ]])

            caption = (
                f"💳 Deposit Request #{dep_id}\n\n"
                f"User: {user.full_name}\n"
                f"Username: @{user.username or 'N/A'}\n"
                f"User ID: {user.id}\n"
                f"Method: {method}\n"
                f"Requested Amount: {amount} AFN\n"
                f"Fee: {fee_amount} AFN\n"
                f"Add to Balance: {final_amount} AFN"
            )

            await context.bot.send_photo(
                chat_id=ADMIN_ID,
                photo=proof_file_id,
                caption=caption,
                reply_markup=buttons
            )

            await update.message.reply_text(t(user.id, "deposit_sent"))
            context.user_data.clear()
            return

    # withdraw
    if flow == "withdraw":
        step = context.user_data.get("withdraw_step")

        if step == "amount":
            try:
                amount = int(update.message.text.strip())
                if amount < 50:
                    await update.message.reply_text(t(user.id, "withdraw_min"))
                    context.user_data.clear()
                    return
            except Exception:
                await update.message.reply_text("Enter a valid amount.")
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
            context.user_data["withdraw_name"] = update.message.text.strip()
            context.user_data["withdraw_step"] = "phone"
            await update.message.reply_text(t(user.id, "withdraw_ask_phone"))
            return

        if step == "phone":
            context.user_data["withdraw_phone"] = update.message.text.strip()
            context.user_data["withdraw_step"] = "network"
            await update.message.reply_text(t(user.id, "withdraw_ask_network"))
            return

        if step == "network":
            amount = context.user_data["withdraw_amount"]
            full_name = context.user_data["withdraw_name"]
            phone = context.user_data["withdraw_phone"]
            network = update.message.text.strip()

            change_balance(user.id, -amount)

            cur.execute("""
                INSERT INTO withdrawals (user_id, amount, network, phone, full_name, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (user.id, amount, network, phone, full_name, now_iso()))
            conn.commit()
            wd_id = cur.lastrowid

            final_amount, fee_amount = withdraw_fee(amount)

            buttons = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Approve", callback_data=f"admin_approve_withdraw_{wd_id}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"admin_reject_withdraw_{wd_id}"),
            ]])

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
                reply_markup=buttons
            )

            await update.message.reply_text(t(user.id, "withdraw_sent"))
            context.user_data.clear()
            return

    # create campaign
    if flow == "create_campaign":
        step = context.user_data.get("campaign_step")
        campaign_type = context.user_data.get("campaign_type")

        if step == "link":
            context.user_data["campaign_link"] = update.message.text.strip()
            context.user_data["campaign_step"] = "username"
            await update.message.reply_text(t(user.id, "ask_username"))
            return

        if step == "username":
            username = update.message.text.strip()
            link = context.user_data["campaign_link"]

            reward = 2 if campaign_type == "channel" else 1
            title_ps = "چینل جوین کړئ" if campaign_type == "channel" else "ګروپ جوین کړئ"
            title_en = "Join channel" if campaign_type == "channel" else "Join group"

            cur.execute("""
                INSERT INTO campaigns (
                    owner_user_id, title_ps, title_en, reward_afn,
                    target_type, link, chat_username, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user.id,
                title_ps,
                title_en,
                reward,
                campaign_type,
                link,
                username,
                now_iso()
            ))
            conn.commit()

            await update.message.reply_text(t(user.id, "campaign_created"))
            context.user_data.clear()
            return

# =========================
# MAIN
# =========================
def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(buttons))
    app.add_handler(MessageHandler((filters.TEXT | filters.PHOTO) & ~filters.COMMAND, flow_router))

    print("EasyEarn bot is running...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
