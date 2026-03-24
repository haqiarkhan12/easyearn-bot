import os
import sqlite3
from telegram import *
from telegram.ext import *

TOKEN = os.getenv("8713775500:AAE1XxzR3T6BKp22HmsCc9NU7cZg-htE6Bc")
ADMIN_ID = int(os.getenv("1347546821"))

CHANNEL_USERNAME = "easyearnofficial1222"  # ستا چینل

# ================= DB =================
conn = sqlite3.connect("db.sqlite3", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    balance INTEGER DEFAULT 0,
    referrer INTEGER
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel TEXT,
    reward INTEGER
)
""")

conn.commit()

# ================= START =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user

    cursor.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user.id,))
    conn.commit()

    # force join check
    member = await context.bot.get_chat_member(f"@{CHANNEL_USERNAME}", user.id)
    if member.status not in ["member", "administrator", "creator"]:
        keyboard = [
            [InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{CHANNEL_USERNAME}")],
            [InlineKeyboardButton("✅ I Joined", callback_data="check_join")]
        ]
        await update.message.reply_text(
            "🚀 Please join our channel first:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    await main_menu(update, context)

# ================= JOIN CHECK =================
async def check_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user

    member = await context.bot.get_chat_member(f"@{CHANNEL_USERNAME}", user.id)

    if member.status in ["member", "administrator", "creator"]:
        await query.message.delete()
        await main_menu(query, context)
    else:
        await query.answer("❌ You didn't join", show_alert=True)

# ================= MENU =================
async def main_menu(update, context):
    keyboard = [
        [InlineKeyboardButton("💰 Balance", callback_data="balance"),
         InlineKeyboardButton("📢 Tasks", callback_data="tasks")],
        [InlineKeyboardButton("🎁 Daily Bonus", callback_data="bonus"),
         InlineKeyboardButton("👥 Referral", callback_data="referral")],
        [InlineKeyboardButton("💳 Deposit", callback_data="deposit"),
         InlineKeyboardButton("💸 Withdraw", callback_data="withdraw")]
    ]

    if isinstance(update, Update):
        await update.message.reply_text("🎮 Welcome to EasyEarn Bot",
            reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text("🎮 Welcome back",
            reply_markup=InlineKeyboardMarkup(keyboard))

# ================= BALANCE =================
async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id

    cursor.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
    bal = cursor.fetchone()[0]

    await query.answer()
    await query.message.reply_text(f"💰 Your balance: {bal} AFN")

# ================= TASKS =================
async def tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    cursor.execute("SELECT * FROM tasks")
    tasks = cursor.fetchall()

    if not tasks:
        await query.message.reply_text("❌ No tasks available")
        return

    for t in tasks:
        keyboard = [
            [InlineKeyboardButton("🔗 Join", url=f"https://t.me/{t[1]}")],
            [InlineKeyboardButton("✅ Verify", callback_data=f"verify_{t[0]}")]
        ]
        await query.message.reply_text(
            f"📢 Join @{t[1]}\n💰 Reward: {t[2]} AFN",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

# ================= VERIFY =================
async def verify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user

    task_id = int(query.data.split("_")[1])

    cursor.execute("SELECT channel, reward FROM tasks WHERE id=?", (task_id,))
    t = cursor.fetchone()

    member = await context.bot.get_chat_member(f"@{t[0]}", user.id)

    if member.status in ["member", "administrator", "creator"]:
        cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (t[1], user.id))
        conn.commit()
        await query.answer("✅ Done! Reward added")
    else:
        await query.answer("❌ Join first")

# ================= BONUS =================
async def bonus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id

    cursor.execute("UPDATE users SET balance = balance + 1 WHERE user_id=?", (user_id,))
    conn.commit()

    await query.answer()
    await query.message.reply_text("🎁 You got 1 AFN!")

# ================= REFERRAL =================
async def referral(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id

    link = f"https://t.me/EasyEarnAppBot?start={user_id}"

    await query.answer()
    await query.message.reply_text(f"👥 Your link:\n{link}\n\nEarn 2 AFN per referral")

# ================= DEPOSIT =================
async def deposit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    keyboard = [
        [InlineKeyboardButton("Hesab Pay", callback_data="hesab")],
        [InlineKeyboardButton("Atoma Pay", callback_data="atoma")]
    ]

    await query.answer()
    await query.message.reply_text("Choose method:", reply_markup=InlineKeyboardMarkup(keyboard))

async def deposit_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    method = query.data
    number = "+93708310201" if method == "hesab" else "+93770876916"

    context.user_data["deposit"] = {"method": method}

    await query.answer()
    await query.message.reply_text(f"Send money to:\n{number}\n\nEnter amount:")

# ================= MAIN =================
def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(check_join, pattern="check_join"))
    app.add_handler(CallbackQueryHandler(balance, pattern="balance"))
    app.add_handler(CallbackQueryHandler(tasks, pattern="tasks"))
    app.add_handler(CallbackQueryHandler(verify, pattern="verify_"))
    app.add_handler(CallbackQueryHandler(bonus, pattern="bonus"))
    app.add_handler(CallbackQueryHandler(referral, pattern="referral"))
    app.add_handler(CallbackQueryHandler(deposit, pattern="deposit"))
    app.add_handler(CallbackQueryHandler(deposit_method, pattern="hesab|atoma"))

    print("Bot Running...")
    app.run_polling()

if __name__ == "__main__":
    main()
