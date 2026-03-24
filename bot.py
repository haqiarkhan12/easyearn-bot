import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL = "@easyearnofficial1222"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🇦🇫 پښتو", callback_data="ps")],
        [InlineKeyboardButton("🇬🇧 English", callback_data="en")]
    ]
    await update.message.reply_text("ژبه انتخاب کړئ:", reply_markup=InlineKeyboardMarkup(keyboard))

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data in ["ps", "en"]:
        context.user_data["lang"] = query.data

        keyboard = [
            [InlineKeyboardButton("📢 Join Channel", url="https://t.me/easyearnofficial1222")],
            [InlineKeyboardButton("✅ I Joined", callback_data="check")]
        ]

        await query.edit_message_text(
            "اول چینل جوین کړه:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif query.data == "check":
        user_id = query.from_user.id
        member = await context.bot.get_chat_member(CHANNEL, user_id)

        if member.status in ["member", "administrator", "creator"]:
            keyboard = [
                [InlineKeyboardButton("💰 Balance", callback_data="balance")],
                [InlineKeyboardButton("📢 Tasks", callback_data="tasks")]
            ]

            await query.edit_message_text(
                "Main Menu:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await query.answer("اول چینل جوین کړه!", show_alert=True)

def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button))

    print("Bot running...")
    app.run_polling()

if __name__ == "__main__":
    main()
