# bot.py (async + PostgreSQL)
import logging
import re
import os
from datetime import date, timedelta
from io import BytesIO
from dateutil import parser as dateparser
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes,
    MessageHandler, filters, ConversationHandler
)
import db_utils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


bot_token="8487778944:AAEUiyCZM5pTnSxDO5mQJ_mpxm5pqIL7ED4"
bot_user_name="jarviz_money_control_bot"

# Conversation states
CAT, AMT, DTE, DESC = range(4)

async def init_db():
    """Initialize PostgreSQL database - run once at startup."""
    await db_utils.init_db()

async def insert_tx(user_id, category, amount, date_str, description=None, tags=None, currency='INR'):
    """Async insert using PostgreSQL."""
    await db_utils.insert_tx(user_id, category, amount, date_str, description, tags, currency)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hi! I'm your Expense Tracker Bot.\n\n"
        "Commands:\n"
        "/add - interactive add\n"
        "/quick <category> <amount> [free text] --desc \"your description\"\n"
        "/list [n] - last n items\n"
        "/summary [today|week|month|all]\n"
        "/export - get CSV\n"
        "/help - this message"
    )

# /add interactive flow
async def add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Enter category (e.g. food, petrol, creditcard, emi):")
    return CAT

async def cat_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['category'] = update.message.text.strip()
    await update.message.reply_text("Enter amount (numbers):")
    return AMT

async def amt_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        amount = float(re.sub(r'[^\d\.\-]', '', text))
    except:
        await update.message.reply_text("Couldn't parse amount. Enter numeric amount:")
        return AMT
    context.user_data['amount'] = amount
    await update.message.reply_text("Enter date (YYYY-MM-DD) or text like 'today' or '2025-11-13':")
    return DTE

async def date_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip()
    try:
        d = dateparser.parse(txt, fuzzy=True).date()
    except:
        await update.message.reply_text("Couldn't parse date. Please try again:")
        return DTE
    context.user_data['date'] = d.isoformat()
    await update.message.reply_text("Enter description (optional):")
    return DESC

async def desc_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    desc = update.message.text.strip()
    user = update.effective_user
    await insert_tx(user.id, context.user_data['category'], context.user_data['amount'],
              context.user_data['date'], description=desc)
    await update.message.reply_text("Saved ✅")
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END

# /quick parser
QUICK_RE = re.compile(r'(?P<category>[\w-]+)\s+(?P<amount>[\d,]+(?:\.\d+)?)\s*(?P<rest>.*)', re.I)

async def quick_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    payload = text.replace('/quick', '', 1).strip()
    m = QUICK_RE.match(payload)
    if not m:
        await update.message.reply_text("Use: /quick <category> <amount> [free text] --desc \"...\"")
        return
    category = m.group('category')
    amount = float(m.group('amount').replace(',', ''))
    rest = m.group('rest').strip()
    desc = None
    desc_match = re.search(r'--desc\s+"([^"]+)"', rest)
    if desc_match:
        desc = desc_match.group(1)
        rest = rest.replace(desc_match.group(0), '')
    parsed_date = date.today()
    try:
        parsed = dateparser.parse(rest, fuzzy=True)
        if parsed:
            parsed_date = parsed.date()
    except:
        pass
    await insert_tx(update.effective_user.id, category, amount, parsed_date.isoformat(), description=desc, tags=None)
    await update.message.reply_text(f"Saved: {category} {amount} on {parsed_date.isoformat()} ✅")

# /list command
async def list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = (update.message.text or "").split()
    n = 10
    if len(parts) >= 2:
        try:
            n = int(parts[1])
        except:
            pass
    rows = await db_utils.get_transactions(update.effective_user.id, n)
    if not rows:
        await update.message.reply_text("No transactions yet.")
        return
    lines = []
    for r in rows:
        tid, cat, amt, dt, desc = r
        lines.append(f"{dt} | {cat} | {amt} | {desc or ''} (id:{tid})")
    await update.message.reply_text("\n".join(lines))

# /summary - simple totals
async def summary_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = (update.message.text or "").split()
    period = 'month'
    if len(parts) >= 2:
        period = parts[1].lower()
    today = date.today()
    if period == 'today':
        start = today.isoformat()
    elif period == 'week':
        start = (today - timedelta(days=7)).isoformat()
    elif period == 'all':
        start = None
    else:
        start = today.replace(day=1).isoformat()
    rows = await db_utils.get_summary(update.effective_user.id, start)
    if not rows:
        await update.message.reply_text("No transactions for the selected period.")
        return
    msg = "Summary:\n" + "\n".join([f"{r[0]} : {r[1]}" for r in rows])
    await update.message.reply_text(msg)

# /export CSV
async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = await db_utils.get_export_data(update.effective_user.id)
    if not rows:
        await update.message.reply_text("No data to export.")
        return
    csv_lines = ["id,date,category,amount,currency,description"]
    for r in rows:
        desc = (r[5] or "").replace('"', '""')
        csv_lines.append(','.join([str(r[0]), r[1], r[2], str(r[3]), r[4], f'"{desc}"']))
    csv_data = "\n".join(csv_lines)
    csv_bytes = BytesIO(csv_data.encode('utf-8'))
    csv_bytes.seek(0)
    csv_bytes.name = "expenses.csv"
    await update.message.reply_document(document=csv_bytes, filename="expenses.csv")

def get_token_from_file():
    try:
        with open("credentials.txt", "r") as f:
            for line in f:
                if line.strip().startswith("bot_token="):
                    return line.split("=", 1)[1].strip()
    except FileNotFoundError:
        return None

def main():
    # Initialize database before starting the bot
    import asyncio
    asyncio.run(init_db())
    
    TOKEN = os.getenv("BOT_TOKEN") or get_token_from_file() or "REPLACE_WITH_YOUR_BOT_TOKEN"
    if TOKEN == "REPLACE_WITH_YOUR_BOT_TOKEN":
        logger.error("Bot token not provided. Set BOT_TOKEN env var or put bot_token=... in credentials.txt")
    app = ApplicationBuilder().token(TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('add', add_cmd)],
        states={
            CAT: [MessageHandler(filters.TEXT & ~filters.COMMAND, cat_handler)],
            AMT: [MessageHandler(filters.TEXT & ~filters.COMMAND, amt_handler)],
            DTE: [MessageHandler(filters.TEXT & ~filters.COMMAND, date_handler)],
            DESC: [MessageHandler(filters.TEXT & ~filters.COMMAND, desc_handler)],
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", start))
    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("quick", quick_cmd))
    app.add_handler(CommandHandler("list", list_cmd))
    app.add_handler(CommandHandler("summary", summary_cmd))
    app.add_handler(CommandHandler("export", export_cmd))
    
    # Run the bot (this manages its own event loop)
    app.run_polling()

if __name__ == "__main__":
    main()
