import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler, \
    CallbackQueryHandler
import json
from datetime import datetime, timedelta
import pandas as pd
from collections import defaultdict
import calendar
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Conversation states
AMOUNT, DESCRIPTION, MEAL_DATA, MEAL_CONFIRM = range(4)
ADMIN_EXPENSE_MEMBER, ADMIN_EXPENSE_AMOUNT, ADMIN_EXPENSE_DESC = range(4, 7)
EDIT_MEAL_SELECT, EDIT_MEAL_COUNT = range(7, 9)

# Data storage file
DATA_FILE = 'mess_fund_groups.json'


class MessFundManager:
    def __init__(self):
        self.data = self.load_data()

    def load_data(self):
        """Load data from JSON file"""
        try:
            with open(DATA_FILE, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def save_data(self):
        """Save data to JSON file"""
        with open(DATA_FILE, 'w') as f:
            json.dump(self.data, f, indent=2)

    def init_group(self, group_id):
        """Initialize data structure for a new group"""
        group_id = str(group_id)
        if group_id not in self.data:
            self.data[group_id] = {
                'group_name': '',
                'members': {},
                'expenses': [],
                'meal_counts': {},
                'current_month': datetime.now().strftime('%Y-%m'),
                'settlements': [],
                'carry_forward': 0,
                'created_date': datetime.now().strftime('%Y-%m-%d'),
                'meal_data_submitted': False
            }
            self.save_data()
        else:
            # Update existing group with missing fields (backward compatibility)
            updated = False
            if 'meal_counts' not in self.data[group_id]:
                self.data[group_id]['meal_counts'] = {}
                updated = True
            if 'meal_data_submitted' not in self.data[group_id]:
                self.data[group_id]['meal_data_submitted'] = False
                updated = True
            if 'carry_forward' not in self.data[group_id]:
                self.data[group_id]['carry_forward'] = 0
                updated = True
            if 'current_month' not in self.data[group_id]:
                self.data[group_id]['current_month'] = datetime.now().strftime('%Y-%m')
                updated = True
            if 'settlements' not in self.data[group_id]:
                self.data[group_id]['settlements'] = []
                updated = True
            if updated:
                self.save_data()

    def add_member(self, group_id, name, user_id, username=''):
        """Add or update a member"""
        group_id = str(group_id)
        self.init_group(group_id)

        if str(user_id) not in self.data[group_id]['members']:
            self.data[group_id]['members'][str(user_id)] = {
                'name': name,
                'username': username,
                'joined_date': datetime.now().strftime('%Y-%m-%d')
            }
            self.save_data()
            return True
        return False

    def add_expense(self, group_id, amount, description, added_by, added_by_id):
        """Record an expense"""
        group_id = str(group_id)
        self.init_group(group_id)

        expense = {
            'amount': amount,
            'description': description,
            'added_by': added_by,
            'added_by_id': str(added_by_id),
            'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'month': datetime.now().strftime('%Y-%m')
        }
        self.data[group_id]['expenses'].append(expense)
        self.save_data()

    def set_meal_counts(self, group_id, meal_data, submitted_by):
        """Set meal counts for all members"""
        group_id = str(group_id)
        self.init_group(group_id)
        current_month = datetime.now().strftime('%Y-%m')

        if current_month not in self.data[group_id]['meal_counts']:
            self.data[group_id]['meal_counts'][current_month] = {}

        self.data[group_id]['meal_counts'][current_month] = {
            'data': meal_data,
            'submitted_by': submitted_by,
            'submitted_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        self.data[group_id]['meal_data_submitted'] = True
        self.save_data()

    def update_single_meal_count(self, group_id, user_id, new_count):
        """Update meal count for a single member"""
        group_id = str(group_id)
        self.init_group(group_id)
        current_month = datetime.now().strftime('%Y-%m')

        if current_month not in self.data[group_id]['meal_counts']:
            return False

        if 'data' not in self.data[group_id]['meal_counts'][current_month]:
            return False

        self.data[group_id]['meal_counts'][current_month]['data'][user_id] = new_count
        self.save_data()
        return True

    def get_current_month_expenses(self, group_id):
        """Get expenses for current month"""
        group_id = str(group_id)
        self.init_group(group_id)
        current_month = datetime.now().strftime('%Y-%m')

        return [e for e in self.data[group_id]['expenses']
                if e.get('month', '') == current_month]

    def get_current_month_meals(self, group_id):
        """Get meal counts for current month"""
        group_id = str(group_id)
        self.init_group(group_id)
        current_month = datetime.now().strftime('%Y-%m')

        meal_counts = self.data[group_id].get('meal_counts', {})
        if current_month in meal_counts:
            return meal_counts[current_month].get('data', {})
        return {}

    def is_meal_data_submitted(self, group_id):
        """Check if meal data submitted"""
        group_id = str(group_id)
        self.init_group(group_id)
        return self.data[group_id].get('meal_data_submitted', False)

    def calculate_settlement(self, group_id):
        """Calculate settlement"""
        group_id = str(group_id)
        self.init_group(group_id)
        current_month = datetime.now().strftime('%Y-%m')

        expenses = self.get_current_month_expenses(group_id)
        meal_data = self.get_current_month_meals(group_id)

        if not expenses or not meal_data:
            return None

        carry_forward = self.data[group_id].get('carry_forward', 0)
        total_expenses = sum(e['amount'] for e in expenses)
        total_with_carry = total_expenses + carry_forward
        total_meals = sum(meal_data.values())

        if total_meals == 0:
            return None

        cost_per_meal = total_with_carry / total_meals

        member_spent = {}
        for expense in expenses:
            user_id = expense['added_by_id']
            name = expense['added_by']
            if user_id not in member_spent:
                member_spent[user_id] = {'name': name, 'spent': 0}
            member_spent[user_id]['spent'] += expense['amount']

        settlements = []

        for user_id, meal_count in meal_data.items():
            spent = member_spent.get(user_id, {}).get('spent', 0)
            owes = meal_count * cost_per_meal

            if user_id in self.data[group_id]['members']:
                name = self.data[group_id]['members'][user_id]['name']
            else:
                name = member_spent.get(user_id, {}).get('name', f'Member {user_id}')

            balance = spent - owes

            settlements.append({
                'user_id': user_id,
                'name': name,
                'spent': spent,
                'meals': meal_count,
                'owes': owes,
                'balance': balance
            })

        for user_id, data in member_spent.items():
            if user_id not in meal_data:
                settlements.append({
                    'user_id': user_id,
                    'name': data['name'],
                    'spent': data['spent'],
                    'meals': 0,
                    'owes': 0,
                    'balance': data['spent']
                })

        remaining = total_expenses - (cost_per_meal * total_meals)

        return {
            'month': current_month,
            'carry_forward': carry_forward,
            'total_expenses': total_expenses,
            'total_with_carry': total_with_carry,
            'total_meals': total_meals,
            'cost_per_meal': cost_per_meal,
            'settlements': settlements,
            'remaining': remaining
        }

    def reset_month(self, group_id):
        """Archive and reset"""
        group_id = str(group_id)
        self.init_group(group_id)

        settlement = self.calculate_settlement(group_id)

        if settlement:
            self.data[group_id]['settlements'].append({
                'settlement': settlement,
                'archived_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            self.data[group_id]['carry_forward'] = settlement.get('remaining', 0)

        current_month = datetime.now().strftime('%Y-%m')
        self.data[group_id]['expenses'] = [
            e for e in self.data[group_id]['expenses']
            if e.get('month', '') != current_month
        ]

        self.data[group_id]['meal_data_submitted'] = False
        self.data[group_id]['current_month'] = datetime.now().strftime('%Y-%m')
        self.save_data()
        return settlement

    def get_all_active_groups(self):
        """Get all groups"""
        return list(self.data.keys())

    def get_member_stats(self, group_id, user_id):
        """Get member stats"""
        group_id = str(group_id)
        user_id = str(user_id)
        self.init_group(group_id)

        expenses = self.get_current_month_expenses(group_id)
        meal_data = self.get_current_month_meals(group_id)

        spent = sum(e['amount'] for e in expenses if e['added_by_id'] == user_id)
        meals_count = meal_data.get(user_id, 0)

        return {
            'spent': spent,
            'meals': meals_count,
            'expense_count': sum(1 for e in expenses if e['added_by_id'] == user_id)
        }

    def export_to_csv(self, group_id):
        """Export to CSV"""
        group_id = str(group_id)
        self.init_group(group_id)
        files = []

        expenses = self.get_current_month_expenses(group_id)
        if expenses:
            df = pd.DataFrame(expenses)
            filename = f'expenses_{group_id}.csv'
            df.to_csv(filename, index=False)
            files.append(filename)

        meal_data = self.get_current_month_meals(group_id)
        if meal_data:
            meal_list = [{'user_id': k, 'meal_count': v} for k, v in meal_data.items()]
            df = pd.DataFrame(meal_list)
            filename = f'meals_{group_id}.csv'
            df.to_csv(filename, index=False)
            files.append(filename)

        return files


# Initialize manager
manager = MessFundManager()

# Global bot instance and scheduler
bot_instance = None
scheduler = None


async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if user is admin"""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    try:
        chat_admins = await context.bot.get_chat_administrators(chat_id)
        return any(admin.user.id == user_id for admin in chat_admins)
    except Exception as e:
        logger.error(f"Error checking admin status: {e}")
        return False


async def check_meal_reminder():
    """Check if meal data reminder needed"""
    now = datetime.now()
    last_day = calendar.monthrange(now.year, now.month)[1]
    days_left = last_day - now.day

    if days_left == 3:
        logger.info("Sending meal data reminders...")
        active_groups = manager.get_all_active_groups()

        for group_id in active_groups:
            try:
                if not manager.is_meal_data_submitted(group_id):
                    await bot_instance.send_message(
                        chat_id=int(group_id),
                        text="⚠️ *REMINDER: Meal Data Needed!*\n\n"
                             "Only 3 days left in this month!\n\n"
                             "Please submit meal counts using /addmeals",
                        parse_mode='Markdown'
                    )
            except Exception as e:
                logger.error(f"Error sending reminder: {e}")


async def check_month_end():
    """Check month end"""
    now = datetime.now()
    last_day = calendar.monthrange(now.year, now.month)[1]

    if now.day == last_day:
        logger.info("Month end! Processing settlements...")
        active_groups = manager.get_all_active_groups()

        for group_id in active_groups:
            try:
                if not manager.is_meal_data_submitted(group_id):
                    await bot_instance.send_message(
                        chat_id=int(group_id),
                        text="⚠️ *MONTH END - NO MEAL DATA!*\n\n"
                             "Cannot calculate settlement without meal data.\n\n"
                             "Please use /addmeals now!",
                        parse_mode='Markdown'
                    )
                    continue

                settlement = manager.calculate_settlement(group_id)

                if settlement:
                    text = f"📅 *MONTH END SETTLEMENT*\n\n"
                    text += f"📆 {settlement['month']}\n\n"

                    if settlement['carry_forward'] != 0:
                        text += f"💰 Carry Forward: ₹{settlement['carry_forward']:.2f}\n"

                    text += f"💸 Total Expenses: ₹{settlement['total_expenses']:.2f}\n"
                    text += f"🍽️ Total Meals: {settlement['total_meals']}\n"
                    text += f"💵 Cost per Meal: ₹{settlement['cost_per_meal']:.2f}\n\n"

                    text += "👥 *SETTLEMENT:*\n"
                    text += "━━━━━━━━━━━━━━━━\n\n"

                    for s in sorted(settlement['settlements'], key=lambda x: x['balance'], reverse=True):
                        text += f"👤 *{s['name']}*\n"
                        text += f"   💸 Spent: ₹{s['spent']:.2f}\n"
                        text += f"   🍽️ Meals: {s['meals']}\n"
                        text += f"   💵 Owes: ₹{s['owes']:.2f}\n"

                        if s['balance'] > 0:
                            text += f"   ✅ Gets Back: ₹{s['balance']:.2f}\n\n"
                        elif s['balance'] < 0:
                            text += f"   ⚠️ Needs to Pay: ₹{abs(s['balance']):.2f}\n\n"
                        else:
                            text += f"   ✔️ Settled\n\n"

                    if settlement['remaining'] != 0:
                        text += f"\n💰 Remaining: ₹{settlement['remaining']:.2f}\n"
                        text += "(Carried forward to next month)"

                    await bot_instance.send_message(
                        chat_id=int(group_id),
                        text=text,
                        parse_mode='Markdown'
                    )

                    manager.reset_month(group_id)

                    await bot_instance.send_message(
                        chat_id=int(group_id),
                        text="✅ Month has been reset! Start fresh for the new month."
                    )

            except Exception as e:
                logger.error(f"Error processing group {group_id}: {e}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start"""
    chat_type = update.effective_chat.type

    if chat_type == 'private':
        await update.message.reply_text(
            "🍽️ *Mess Fund Manager Bot*\n\n"
            "Perfect for hostel/PG mess management!\n\n"
            "📋 *Features:*\n"
            "✅ Track expenses when members buy items\n"
            "✅ Add meal counts at month end\n"
            "✅ Automatic settlement calculation\n"
            "✅ Fair cost distribution\n"
            "✅ Auto reminders & reports\n"
            "✅ Admin controls\n\n"
            "Add me to your group to get started!",
            parse_mode='Markdown'
        )
    else:
        manager.init_group(update.effective_chat.id)
        await update.message.reply_text(
            "👋 Hello! I'm your Mess Fund Manager.\n\n"
            "✨ I'll send reminders and auto-settlement reports!\n\n"
            "Use /help to see all commands."
        )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help"""
    help_text = (
        "🍽️ *Mess Fund Manager Commands*\n\n"
        "👤 *For All Members:*\n"
        "/register - Register yourself\n"
        "/expense - Add your expense\n"
        "/mystats - View your stats\n"
        "/summary - View month summary\n"
        "/settlement - View settlement\n\n"
        "📊 *Meal Management:*\n"
        "/addmeals - Add meal counts (bulk)\n"
        "/viewmeals - View meal data\n\n"
        "👑 *Admin Only:*\n"
        "/addexpense - Add expense for any member\n"
        "/editmeal - Edit meal count for any member\n"
        "/reset - Manual month reset\n"
        "/export - Export data to CSV\n\n"
        "🤖 *Auto Features:*\n"
        "• Reminder 3 days before month end\n"
        "• Auto settlement on last day\n"
        "• Balance carry forward"
    )
    await update.message.reply_text(help_text, parse_mode='Markdown')


async def register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Register"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text("⚠️ This command only works in groups!")
        return

    user = update.effective_user
    group_id = update.effective_chat.id

    added = manager.add_member(group_id, user.first_name, user.id, user.username or '')

    msg = f"✅ Welcome {user.first_name}! You're now registered." if added else f"ℹ️ You're already registered, {user.first_name}!"
    await update.message.reply_text(msg)


async def expense_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Expense start"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text("⚠️ This command only works in groups!")
        return ConversationHandler.END

    user = update.effective_user
    group_id = update.effective_chat.id

    manager.add_member(group_id, user.first_name, user.id, user.username or '')

    context.user_data['action'] = 'expense'
    context.user_data['group_id'] = group_id
    context.user_data['user_id'] = user.id
    context.user_data['user_name'] = user.first_name

    await update.message.reply_text(f"💸 Hi {user.first_name}! Enter the amount you spent (₹):")
    return AMOUNT


async def amount_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Amount"""
    try:
        amount = float(update.message.text)
        if amount <= 0:
            raise ValueError

        context.user_data['amount'] = amount
        await update.message.reply_text('📝 What did you buy? (e.g., vegetables, rice, etc.)')
        return DESCRIPTION
    except ValueError:
        await update.message.reply_text('❌ Invalid amount! Please enter a valid number.')
        return AMOUNT


async def description_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Description"""
    description = update.message.text
    amount = context.user_data['amount']
    group_id = context.user_data['group_id']
    user_id = context.user_data['user_id']
    user_name = context.user_data['user_name']

    manager.add_expense(group_id, amount, description, user_name, user_id)
    stats = manager.get_member_stats(group_id, user_id)

    await update.message.reply_text(
        f"✅ *Expense Recorded!*\n\n"
        f"💸 Amount: ₹{amount:.2f}\n"
        f"📝 Item: {description}\n"
        f"👤 By: {user_name}\n\n"
        f"📊 *Your Month Stats:*\n"
        f"💰 Total Spent: ₹{stats['spent']:.2f}\n"
        f"🍽️ Your Meals: {stats['meals']}",
        parse_mode='Markdown'
    )

    context.user_data.clear()
    return ConversationHandler.END


# ADMIN FEATURE 1: Add expense for any member
async def admin_add_expense_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin add expense for any member"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text("⚠️ This command only works in groups!")
        return ConversationHandler.END

    if not await is_admin(update, context):
        await update.message.reply_text("⚠️ Only admins can use this command!")
        return ConversationHandler.END

    group_id = str(update.effective_chat.id)
    members = manager.data[group_id].get('members', {})

    if not members:
        await update.message.reply_text("⚠️ No members registered yet!")
        return ConversationHandler.END

    context.user_data['group_id'] = update.effective_chat.id

    # Create buttons for member selection
    keyboard = []
    for uid, info in members.items():
        keyboard.append([InlineKeyboardButton(info['name'], callback_data=f"adminexp_{uid}")])
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="adminexp_cancel")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "👑 *Admin: Add Expense*\n\n"
        "Select the member who bought items:",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

    return ADMIN_EXPENSE_MEMBER


async def admin_expense_member_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle member selection for admin expense"""
    query = update.callback_query
    await query.answer()

    if query.data == "adminexp_cancel":
        await query.edit_message_text("❌ Cancelled.")
        context.user_data.clear()
        return ConversationHandler.END

    user_id = query.data.replace("adminexp_", "")
    group_id = str(context.user_data['group_id'])

    member_name = manager.data[group_id]['members'][user_id]['name']

    context.user_data['selected_user_id'] = user_id
    context.user_data['selected_user_name'] = member_name

    await query.edit_message_text(
        f"👤 Selected: *{member_name}*\n\n"
        f"💸 Enter the amount spent (₹):",
        parse_mode='Markdown'
    )

    return ADMIN_EXPENSE_AMOUNT


async def admin_expense_amount_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle amount for admin expense"""
    try:
        amount = float(update.message.text)
        if amount <= 0:
            raise ValueError

        context.user_data['amount'] = amount
        await update.message.reply_text('📝 What was purchased? (description)')
        return ADMIN_EXPENSE_DESC
    except ValueError:
        await update.message.reply_text('❌ Invalid amount! Enter a valid number.')
        return ADMIN_EXPENSE_AMOUNT


async def admin_expense_desc_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Complete admin expense entry"""
    description = update.message.text
    amount = context.user_data['amount']
    group_id = context.user_data['group_id']
    user_id = context.user_data['selected_user_id']
    user_name = context.user_data['selected_user_name']

    manager.add_expense(group_id, amount, description, user_name, user_id)

    await update.message.reply_text(
        f"✅ *Expense Added by Admin!*\n\n"
        f"👤 Member: {user_name}\n"
        f"💸 Amount: ₹{amount:.2f}\n"
        f"📝 Item: {description}\n\n"
        f"✨ Added by: {update.effective_user.first_name}",
        parse_mode='Markdown'
    )

    context.user_data.clear()
    return ConversationHandler.END


# ADMIN FEATURE 2: Edit meal count
async def admin_edit_meal_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin edit meal count"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text("⚠️ This command only works in groups!")
        return ConversationHandler.END

    if not await is_admin(update, context):
        await update.message.reply_text("⚠️ Only admins can use this command!")
        return ConversationHandler.END

    group_id = str(update.effective_chat.id)

    if not manager.is_meal_data_submitted(group_id):
        await update.message.reply_text("⚠️ No meal data submitted yet! Use /addmeals first.")
        return ConversationHandler.END

    meal_data = manager.get_current_month_meals(group_id)
    members = manager.data[group_id].get('members', {})

    context.user_data['group_id'] = update.effective_chat.id

    # Create buttons for member selection
    keyboard = []
    for uid, count in meal_data.items():
        name = members.get(uid, {}).get('name', f'User {uid}')
        keyboard.append([InlineKeyboardButton(f"{name} (current: {count})", callback_data=f"editmeal_{uid}")])
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="editmeal_cancel")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "👑 *Admin: Edit Meal Count*\n\n"
        "Select member to edit:",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

    return EDIT_MEAL_SELECT


async def admin_edit_meal_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle member selection for meal edit"""
    query = update.callback_query
    await query.answer()

    if query.data == "editmeal_cancel":
        await query.edit_message_text("❌ Cancelled.")
        context.user_data.clear()
        return ConversationHandler.END

    user_id = query.data.replace("editmeal_", "")
    group_id = str(context.user_data['group_id'])

    members = manager.data[group_id].get('members', {})
    meal_data = manager.get_current_month_meals(group_id)

    member_name = members.get(user_id, {}).get('name', f'User {user_id}')
    current_count = meal_data.get(user_id, 0)

    context.user_data['edit_user_id'] = user_id
    context.user_data['edit_user_name'] = member_name
    context.user_data['current_count'] = current_count

    await query.edit_message_text(
        f"👤 Member: *{member_name}*\n"
        f"🍽️ Current meal count: *{current_count}*\n\n"
        f"Enter new meal count:",
        parse_mode='Markdown'
    )

    return EDIT_MEAL_COUNT


async def admin_edit_meal_count_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Update meal count"""
    try:
        new_count = int(update.message.text)
        if new_count < 0:
            raise ValueError

        group_id = context.user_data['group_id']
        user_id = context.user_data['edit_user_id']
        user_name = context.user_data['edit_user_name']
        old_count = context.user_data['current_count']

        manager.update_single_meal_count(group_id, user_id, new_count)

        await update.message.reply_text(
            f"✅ *Meal Count Updated!*\n\n"
            f"👤 Member: {user_name}\n"
            f"🍽️ Old count: {old_count}\n"
            f"🍽️ New count: {new_count}\n\n"
            f"✨ Updated by: {update.effective_user.first_name}",
            parse_mode='Markdown'
        )

        context.user_data.clear()
        return ConversationHandler.END

    except ValueError:
        await update.message.reply_text('❌ Invalid! Enter a valid number (0 or more).')
        return EDIT_MEAL_COUNT


async def add_meals_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add meals"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text("⚠️ This command only works in groups!")
        return ConversationHandler.END

    group_id = update.effective_chat.id
    user = update.effective_user

    context.user_data['group_id'] = group_id
    context.user_data['submitted_by'] = user.first_name

    group_data = manager.data[str(group_id)]
    members = group_data.get('members', {})

    if not members:
        await update.message.reply_text(
            "⚠️ No members registered yet!\n\n"
            "Members need to use /register first."
        )
        return ConversationHandler.END

    member_list = "\n".join([f"• {info['name']}" for uid, info in members.items()])

    await update.message.reply_text(
        f"📊 *Add Meal Counts - {datetime.now().strftime('%B %Y')}*\n\n"
        f"✅ *Registered Members:*\n{member_list}\n\n"
        f"📝 *Enter meal counts in this format:*\n\n"
        f"*Format:* `Name: count`\n\n"
        f"*Example:*\n"
        f"`Raj: 60\n"
        f"Amit: 55\n"
        f"Priya: 58\n"
        f"Kumar: 62`\n\n"
        f"Type /cancel to abort.",
        parse_mode='Markdown'
    )

    return MEAL_DATA


async def meal_data_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Parse meal data and show confirmation"""
    text = update.message.text
    group_id = context.user_data['group_id']

    meal_data = {}
    matched_names = []
    unmatched_names = []

    lines = text.strip().split('\n')
    group_data = manager.data[str(group_id)]
    members = group_data.get('members', {})

    for line in lines:
        if ':' not in line:
            continue

        parts = line.split(':', 1)
        identifier = parts[0].strip()

        try:
            count = int(parts[1].strip())
            if count < 0:
                raise ValueError

            found = False
            matched_member_name = None

            # Try to match by user_id
            if identifier in members:
                meal_data[identifier] = count
                matched_member_name = members[identifier]['name']
                found = True
            else:
                # Try to match by name (case-insensitive)
                for uid, info in members.items():
                    if info['name'].lower() == identifier.lower():
                        meal_data[uid] = count
                        matched_member_name = info['name']
                        found = True
                        break

            if found:
                matched_names.append(f"{matched_member_name}: {count}")
            else:
                unmatched_names.append(f"{identifier}: {count}")

        except ValueError:
            unmatched_names.append(f"{identifier}: Invalid count")

    if not meal_data:
        await update.message.reply_text(
            "❌ *No valid entries found!*\n\n"
            "Please check:\n"
            "• Correct spelling of names\n"
            "• Format: `Name: count`\n"
            "• Valid numbers only\n\n"
            "Try again or type /cancel",
            parse_mode='Markdown'
        )
        return MEAL_DATA

    # Store parsed data for confirmation
    context.user_data['parsed_meal_data'] = meal_data

    # Build confirmation message
    confirmation_text = "📋 *Please Confirm Meal Data*\n\n"

    if matched_names:
        confirmation_text += "✅ *Matched Members:*\n"
        for entry in matched_names:
            confirmation_text += f"• {entry}\n"
        confirmation_text += f"\n🍽️ Total Meals: {sum(meal_data.values())}\n"

    if unmatched_names:
        confirmation_text += "\n⚠️ *Not Found (will be ignored):*\n"
        for entry in unmatched_names:
            confirmation_text += f"• {entry}\n"
        confirmation_text += "\n💡 *Tip:* Check spelling against registered members list"

    confirmation_text += "\n\n*Is this correct?*"

    # Create inline keyboard for confirmation
    keyboard = [
        [
            InlineKeyboardButton("✅ Yes, Save", callback_data="meal_confirm_yes"),
            InlineKeyboardButton("❌ No, Re-enter", callback_data="meal_confirm_no")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        confirmation_text,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

    return MEAL_CONFIRM


async def meal_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle meal confirmation"""
    query = update.callback_query
    await query.answer()

    if query.data == "meal_confirm_yes":
        # Save the data
        group_id = context.user_data['group_id']
        submitted_by = context.user_data['submitted_by']
        meal_data = context.user_data['parsed_meal_data']

        manager.set_meal_counts(group_id, meal_data, submitted_by)

        # Generate success message
        group_data = manager.data[str(group_id)]
        members = group_data.get('members', {})

        text = f"✅ *Meal Counts Saved Successfully!*\n\n"
        text += f"📅 Month: {datetime.now().strftime('%B %Y')}\n"
        text += f"👤 Submitted by: {submitted_by}\n\n"
        text += "📊 *Summary:*\n"

        total_meals = 0
        for uid, count in meal_data.items():
            name = members.get(uid, {}).get('name', f'User {uid}')
            text += f"• {name}: {count} meals\n"
            total_meals += count

        text += f"\n🍽️ *Total Meals: {total_meals}*"
        text += f"\n\n💡 Use /settlement to view calculations"

        await query.edit_message_text(text, parse_mode='Markdown')

        context.user_data.clear()
        return ConversationHandler.END

    elif query.data == "meal_confirm_no":
        # Ask to re-enter
        await query.edit_message_text(
            "🔄 *Please re-enter meal data*\n\n"
            "Make sure to check spelling!\n\n"
            "Format: `Name: count`\n\n"
            "Or type /cancel to abort",
            parse_mode='Markdown'
        )
        return MEAL_DATA


async def view_meals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View meals"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text("⚠️ This command only works in groups!")
        return

    group_id = str(update.effective_chat.id)

    if not manager.is_meal_data_submitted(group_id):
        await update.message.reply_text(
            "⚠️ No meal data submitted yet for this month!\n\n"
            "Use /addmeals to add meal counts."
        )
        return

    meal_data = manager.get_current_month_meals(group_id)
    members = manager.data[group_id].get('members', {})

    text = f"📊 *Meal Counts - {datetime.now().strftime('%B %Y')}*\n\n"

    total = 0
    for uid, count in meal_data.items():
        name = members.get(uid, {}).get('name', f'User {uid}')
        text += f"• {name}: {count} meals\n"
        total += count

    text += f"\n🍽️ *Total: {total} meals*"

    # Show submission info
    month_data = manager.data[group_id]['meal_counts'].get(datetime.now().strftime('%Y-%m'), {})
    if 'submitted_by' in month_data:
        text += f"\n\n✅ Submitted by: {month_data['submitted_by']}"
        text += f"\n📅 Date: {month_data['submitted_date']}"

    await update.message.reply_text(text, parse_mode='Markdown')


async def my_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stats"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text("⚠️ This command only works in groups!")
        return

    stats = manager.get_member_stats(
        update.effective_chat.id,
        update.effective_user.id
    )

    await update.message.reply_text(
        f"📊 *Your Stats - {datetime.now().strftime('%B %Y')}*\n\n"
        f"👤 {update.effective_user.first_name}\n\n"
        f"💰 Total Spent: ₹{stats['spent']:.2f}\n"
        f"📝 Expenses Added: {stats['expense_count']}\n"
        f"🍽️ Meals Consumed: {stats['meals']}",
        parse_mode='Markdown'
    )


async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Summary"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text("⚠️ This command only works in groups!")
        return

    group_id = str(update.effective_chat.id)

    expenses = manager.get_current_month_expenses(group_id)
    meal_data = manager.get_current_month_meals(group_id)
    carry = manager.data[group_id].get('carry_forward', 0)

    total = sum(e['amount'] for e in expenses)
    total_meals = sum(meal_data.values()) if meal_data else 0

    text = f"📊 *Month Summary - {datetime.now().strftime('%B %Y')}*\n\n"

    if carry != 0:
        text += f"💰 Carry Forward: ₹{carry:.2f}\n"

    text += f"💸 Total Expenses: ₹{total:.2f}\n"
    text += f"📝 Expense Entries: {len(expenses)}\n"
    text += f"🍽️ Total Meals: {total_meals}\n"

    if total_meals > 0:
        text += f"💵 Cost per Meal: ₹{(total + carry) / total_meals:.2f}\n"

    if not manager.is_meal_data_submitted(group_id):
        text += "\n⚠️ Meal data not submitted yet!"

    await update.message.reply_text(text, parse_mode='Markdown')


async def settlement(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Settlement"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text("⚠️ This command only works in groups!")
        return

    group_id = update.effective_chat.id

    if not manager.is_meal_data_submitted(str(group_id)):
        await update.message.reply_text(
            "⚠️ Cannot calculate settlement!\n\n"
            "Meal data not submitted yet. Use /addmeals first."
        )
        return

    result = manager.calculate_settlement(group_id)

    if not result:
        await update.message.reply_text(
            "⚠️ No data available for settlement calculation!\n\n"
            "Make sure both expenses and meals are recorded."
        )
        return

    text = f"💰 *Settlement Calculation*\n\n"
    text += f"📅 Month: {result['month']}\n\n"

    if result['carry_forward'] != 0:
        text += f"💰 Carry Forward: ₹{result['carry_forward']:.2f}\n"

    text += f"💸 Total Expenses: ₹{result['total_expenses']:.2f}\n"
    text += f"🍽️ Total Meals: {result['total_meals']}\n"
    text += f"💵 Cost per Meal: ₹{result['cost_per_meal']:.2f}\n\n"

    text += "👥 *Individual Settlement:*\n"
    text += "━━━━━━━━━━━━━━━━\n\n"

    for s in sorted(result['settlements'], key=lambda x: x['balance'], reverse=True):
        text += f"👤 *{s['name']}*\n"
        text += f"   💸 Spent: ₹{s['spent']:.2f}\n"
        text += f"   🍽️ Meals: {s['meals']}\n"
        text += f"   💵 Owes: ₹{s['owes']:.2f}\n"

        if s['balance'] > 0:
            text += f"   ✅ Gets Back: ₹{s['balance']:.2f}\n\n"
        elif s['balance'] < 0:
            text += f"   ⚠️ Needs to Pay: ₹{abs(s['balance']):.2f}\n\n"
        else:
            text += f"   ✔️ Settled\n\n"

    await update.message.reply_text(text, parse_mode='Markdown')


async def reset_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reset"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text("⚠️ This command only works in groups!")
        return

    if not await is_admin(update, context):
        await update.message.reply_text("⚠️ Only group admins can reset the month!")
        return

    group_id = update.effective_chat.id
    settlement = manager.reset_month(group_id)

    if settlement:
        await update.message.reply_text(
            f"✅ Month has been archived!\n\n"
            f"Settlement for {settlement['month']} saved.\n"
            f"Balance ₹{settlement.get('remaining', 0):.2f} carried forward.\n\n"
            f"Starting fresh for the new month!"
        )
    else:
        await update.message.reply_text("✅ Month reset! No data to archive.")


async def export_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Export"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text("⚠️ This command only works in groups!")
        return

    if not await is_admin(update, context):
        await update.message.reply_text("⚠️ Only group admins can export data!")
        return

    group_id = update.effective_chat.id

    try:
        files = manager.export_to_csv(group_id)

        if not files:
            await update.message.reply_text("⚠️ No data to export yet!")
            return

        await update.message.reply_text("📊 Exporting data...")

        for file in files:
            await update.message.reply_document(
                document=open(file, 'rb'),
                filename=file
            )

        await update.message.reply_text("✅ Data exported successfully!")
    except Exception as e:
        logger.error(f"Export error: {e}")
        await update.message.reply_text(f"❌ Export failed: {str(e)}")


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel"""
    context.user_data.clear()
    await update.message.reply_text("❌ Operation cancelled.")
    return ConversationHandler.END


async def post_init(application: Application):
    """Initialize scheduler after bot starts"""
    global bot_instance, scheduler

    bot_instance = application.bot

    scheduler = AsyncIOScheduler()
    # Meal reminder at 10 AM daily
    scheduler.add_job(check_meal_reminder, 'cron', hour=10, minute=0)
    # Month end check at 11:59 PM daily
    scheduler.add_job(check_month_end, 'cron', hour=23, minute=59)
    scheduler.start()

    logger.info("✅ Scheduler started successfully!")


def main():
    """Main"""
    BOT_TOKEN = '5875408866:AAEzNrmEj3QV7F19TigTISfCkODcwsKqwf8'

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    # Regular expense handler
    expense_handler = ConversationHandler(
        entry_points=[CommandHandler('expense', expense_start)],
        states={
            AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, amount_handler)],
            DESCRIPTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, description_handler)],
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    # Admin add expense handler
    admin_expense_handler = ConversationHandler(
        entry_points=[CommandHandler('addexpense', admin_add_expense_start)],
        states={
            ADMIN_EXPENSE_MEMBER: [CallbackQueryHandler(admin_expense_member_select, pattern='^adminexp_')],
            ADMIN_EXPENSE_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_expense_amount_handler)],
            ADMIN_EXPENSE_DESC: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_expense_desc_handler)],
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    # Admin edit meal handler
    admin_edit_meal_handler = ConversationHandler(
        entry_points=[CommandHandler('editmeal', admin_edit_meal_start)],
        states={
            EDIT_MEAL_SELECT: [CallbackQueryHandler(admin_edit_meal_select, pattern='^editmeal_')],
            EDIT_MEAL_COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_meal_count_handler)],
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    # Meal handler with confirmation
    meal_handler = ConversationHandler(
        entry_points=[CommandHandler('addmeals', add_meals_start)],
        states={
            MEAL_DATA: [MessageHandler(filters.TEXT & ~filters.COMMAND, meal_data_handler)],
            MEAL_CONFIRM: [CallbackQueryHandler(meal_confirm_callback, pattern='^meal_confirm_')]
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('help', help_command))
    application.add_handler(CommandHandler('register', register))
    application.add_handler(expense_handler)
    application.add_handler(admin_expense_handler)
    application.add_handler(admin_edit_meal_handler)
    application.add_handler(meal_handler)
    application.add_handler(CommandHandler('viewmeals', view_meals))
    application.add_handler(CommandHandler('mystats', my_stats))
    application.add_handler(CommandHandler('summary', summary))
    application.add_handler(CommandHandler('settlement', settlement))
    application.add_handler(CommandHandler('reset', reset_month))
    application.add_handler(CommandHandler('export', export_data))

    print('🤖 Mess Fund Manager Bot is running...')
    print('📅 Meal reminder: Daily at 10:00 AM')
    print('📅 Auto settlement: Daily at 11:59 PM')
    print('👑 Admin features enabled')
    print('✨ All systems ready!')

    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
