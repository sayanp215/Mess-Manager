import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler, CallbackQueryHandler
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

# Conversation states - UPDATED WITH NEW STATES
AMOUNT, DESCRIPTION, MEAL_DATA, MEAL_CONFIRM = range(4)
ADMIN_EXPENSE_MEMBER, ADMIN_EXPENSE_AMOUNT, ADMIN_EXPENSE_DESC = range(4, 7)
EDIT_MEAL_SELECT, EDIT_MEAL_COUNT = range(7, 9)
MEAL_MEMBER_SELECT, MEAL_COUNT_INPUT = range(9, 11)

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

    def get_member_wise_summary(self, group_id):
        """Get detailed member-wise summary"""
        group_id = str(group_id)
        self.init_group(group_id)

        expenses = self.get_current_month_expenses(group_id)
        meal_data = self.get_current_month_meals(group_id)
        members = self.data[group_id].get('members', {})

        cost_per_meal = 0
        if expenses and meal_data:
            carry_forward = self.data[group_id].get('carry_forward', 0)
            total_expenses = sum(e['amount'] for e in expenses)
            total_with_carry = total_expenses + carry_forward
            total_meals = sum(meal_data.values())
            if total_meals > 0:
                cost_per_meal = total_with_carry / total_meals

        member_summary = {}

        all_member_ids = set()
        for expense in expenses:
            all_member_ids.add(expense['added_by_id'])
        all_member_ids.update(meal_data.keys())
        all_member_ids.update(members.keys())

        for uid in all_member_ids:
            if uid in members:
                name = members[uid]['name']
                username = members[uid].get('username', '')
            else:
                name = f"User {uid}"
                username = ""

            member_expenses = [e for e in expenses if e['added_by_id'] == uid]
            total_spent = sum(e['amount'] for e in member_expenses)
            expense_count = len(member_expenses)

            meals = meal_data.get(uid, 0)

            owes = meals * cost_per_meal if cost_per_meal > 0 else 0
            balance = total_spent - owes

            member_summary[uid] = {
                'name': name,
                'username': username,
                'total_spent': total_spent,
                'expense_count': expense_count,
                'expense_details': member_expenses,
                'meals': meals,
                'owes': owes,
                'balance': balance
            }

        return member_summary

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

def get_main_menu_keyboard(is_admin=False):
    """Generate main menu keyboard"""
    keyboard = [
        [InlineKeyboardButton("📊 Month Summary", callback_data="menu_summary"),
         InlineKeyboardButton("👥 Members", callback_data="menu_members")],
        [InlineKeyboardButton("💸 Add Expense", callback_data="menu_expense"),
         InlineKeyboardButton("📈 My Stats", callback_data="menu_mystats")],
        [InlineKeyboardButton("💰 Settlement", callback_data="menu_settlement"),
         InlineKeyboardButton("🍽️ View Meals", callback_data="menu_viewmeals")],
        [InlineKeyboardButton("📝 Add Meals", callback_data="menu_addmeals")]
    ]

    if is_admin:
        keyboard.append([
            InlineKeyboardButton("👑 Admin Menu", callback_data="menu_admin")
        ])

    keyboard.append([InlineKeyboardButton("❓ Help", callback_data="menu_help")])

    return InlineKeyboardMarkup(keyboard)

def get_admin_menu_keyboard():
    """Generate admin menu keyboard"""
    keyboard = [
        [InlineKeyboardButton("💸 Add Member Expense", callback_data="admin_addexpense"),
         InlineKeyboardButton("🍽️ Edit Meal Count", callback_data="admin_editmeal")],
        [InlineKeyboardButton("🔄 Reset Month", callback_data="admin_reset"),
         InlineKeyboardButton("📥 Export Data", callback_data="admin_export")],
        [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="menu_main")]
    ]
    return InlineKeyboardMarkup(keyboard)

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
                    keyboard = [[InlineKeyboardButton("📝 Add Meal Data", callback_data="menu_addmeals")]]
                    reply_markup = InlineKeyboardMarkup(keyboard)

                    await bot_instance.send_message(
                        chat_id=int(group_id),
                        text="⚠️ *REMINDER: Meal Data Needed!*\n\n"
                             "Only 3 days left in this month!",
                        parse_mode='Markdown',
                        reply_markup=reply_markup
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
                    keyboard = [[InlineKeyboardButton("📝 Add Meal Data Now", callback_data="menu_addmeals")]]
                    reply_markup = InlineKeyboardMarkup(keyboard)

                    await bot_instance.send_message(
                        chat_id=int(group_id),
                        text="⚠️ *MONTH END - NO MEAL DATA!*\n\n"
                             "Cannot calculate settlement without meal data.",
                        parse_mode='Markdown',
                        reply_markup=reply_markup
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
    """Start command with button menu"""
    chat_type = update.effective_chat.type

    if chat_type == 'private':
        await update.message.reply_text(
            "🍽️ *Mess Fund Manager Bot*\n\n"
            "Perfect for hostel/PG mess management!\n\n"
            "📋 *Features:*\n"
            "✅ Track expenses\n"
            "✅ Add meal counts\n"
            "✅ Auto settlement\n"
            "✅ Fair distribution\n"
            "✅ Admin controls\n"
            "✅ Member-wise summary\n\n"
            "Add me to your group to get started!",
            parse_mode='Markdown'
        )
    else:
        manager.init_group(update.effective_chat.id)

        user_is_admin = await is_admin(update, context)

        await update.message.reply_text(
            "👋 *Welcome to Mess Fund Manager!*\n\n"
            f"📅 Current Month: {datetime.now().strftime('%B %Y')}\n\n"
            "Choose an option below:",
            reply_markup=get_main_menu_keyboard(user_is_admin),
            parse_mode='Markdown'
        )

async def menu_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle menu button callbacks"""
    query = update.callback_query
    await query.answer()

    callback_data = query.data

    if callback_data.startswith("quick_expense_"):
        await quick_expense_callback(update, context)
        return

    if callback_data == "menu_main":
        user_is_admin = await is_admin(update, context)
        await query.edit_message_text(
            "🍽️ *Main Menu*\n\n"
            "Choose an option:",
            reply_markup=get_main_menu_keyboard(user_is_admin),
            parse_mode='Markdown'
        )

    elif callback_data == "menu_help":
        help_text = (
            "🍽️ *Mess Fund Manager*\n\n"
            "📝 *Daily Operations:*\n"
            "• Type any message to get menu\n"
            "• Type a number for quick expense\n"
            "• Track your purchases\n"
            "• Add meal counts at month end\n\n"
            "📊 *Reports:*\n"
            "• View summaries anytime\n"
            "• Check member-wise details\n"
            "• Auto settlement on last day\n\n"
            "👑 *Admin Features:*\n"
            "• Add expenses for any member\n"
            "• Edit meal counts\n"
            "• Manual reset\n"
            "• Export data\n\n"
            "🤖 *Automatic:*\n"
            "• Reminder 3 days before month end\n"
            "• Auto settlement calculation\n\n"
            "💡 *Quick Tips:*\n"
            "• Type 500 → Quick add ₹500 expense\n"
            "• Any text → Shows menu\n"
            "• Use buttons for easy navigation"
        )

        keyboard = [[InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu_main")]]

        await query.edit_message_text(
            help_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

    elif callback_data == "menu_admin":
        if not await is_admin(update, context):
            await query.answer("⚠️ Admin only!", show_alert=True)
            return

        await query.edit_message_text(
            "👑 *Admin Menu*\n\n"
            "Select an admin action:",
            reply_markup=get_admin_menu_keyboard(),
            parse_mode='Markdown'
        )

    elif callback_data == "menu_expense":
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="menu_main")]]
        await query.edit_message_text(
            "💸 *Add Your Expense*\n\n"
            "💡 *Quick way:* Just type a number\n"
            "Example: Type `500`\n\n"
            "📝 *Or use:* /expense command",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

    elif callback_data == "menu_summary":
        await show_summary_from_button(update, context)

    elif callback_data == "menu_members":
        await show_members_from_button(update, context)

    elif callback_data == "menu_mystats":
        await show_mystats_from_button(update, context)

    elif callback_data == "menu_settlement":
        await show_settlement_from_button(update, context)

    elif callback_data == "menu_viewmeals":
        await show_viewmeals_from_button(update, context)

    elif callback_data == "admin_addexpense":
        if not await is_admin(update, context):
            await query.answer("⚠️ Admin only!", show_alert=True)
            return
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="menu_admin")]]
        await query.edit_message_text(
            "👑 *Add Expense for Any Member*\n\n"
            "Use command: /addexpense",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

    elif callback_data == "admin_reset":
        if not await is_admin(update, context):
            await query.answer("⚠️ Admin only!", show_alert=True)
            return
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="menu_admin")]]
        await query.edit_message_text(
            "👑 *Reset Month*\n\n"
            "Use command: /reset\n\n"
            "⚠️ This will archive current data!",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

    elif callback_data == "admin_export":
        if not await is_admin(update, context):
            await query.answer("⚠️ Admin only!", show_alert=True)
            return
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="menu_admin")]]
        await query.edit_message_text(
            "👑 *Export Data*\n\n"
            "Use command: /export",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

async def quick_expense_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle quick expense from number"""
    query = update.callback_query
    await query.answer()

    amount = float(query.data.replace("quick_expense_", ""))

    user = query.from_user
    group_id = query.message.chat_id

    context.user_data['quick_expense_amount'] = amount
    context.user_data['quick_expense_group'] = group_id
    context.user_data['quick_expense_user_id'] = user.id
    context.user_data['quick_expense_user_name'] = user.first_name

    await query.edit_message_text(
        f"💸 *Quick Expense: ₹{amount}*\n\n"
        f"📝 What did you buy?\n\n"
        f"Reply with the description below:",
        parse_mode='Markdown'
    )

async def show_summary_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show summary from button"""
    query = update.callback_query
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
        text += f"💵 Cost per Meal: ₹{(total+carry)/total_meals:.2f}\n"

    if not manager.is_meal_data_submitted(group_id):
        text += "\n⚠️ Meal data not submitted yet!"

    keyboard = [[InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu_main")]]

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def show_members_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show members summary from button"""
    query = update.callback_query
    group_id = str(update.effective_chat.id)

    member_summary = manager.get_member_wise_summary(group_id)

    if not member_summary:
        await query.answer("⚠️ No member data yet!", show_alert=True)
        return

    sorted_members = sorted(
        member_summary.items(),
        key=lambda x: x[1]['balance'],
        reverse=True
    )

    text = f"👥 *MEMBER SUMMARY*\n"
    text += f"📅 {datetime.now().strftime('%B %Y')}\n\n"

    for uid, data in sorted_members:
        text += f"👤 *{data['name']}*\n"
        text += f"💸 ₹{data['total_spent']:.2f} | 🍽️ {data['meals']}\n"

        if data['balance'] > 0:
            text += f"✅ Gets: ₹{data['balance']:.2f}\n\n"
        elif data['balance'] < 0:
            text += f"⚠️ Pays: ₹{abs(data['balance']):.2f}\n\n"
        else:
            text += f"✔️ Settled\n\n"

    text += f"━━━━━━━━━━━━━━\n"
    text += f"📊 Total Members: {len(sorted_members)}"

    keyboard = [[InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu_main")]]

    if len(text) > 4000:
        await query.answer("📊 Sending full summary...", show_alert=False)

        chunk_size = 3500
        chunks = []
        current_chunk = f"👥 *MEMBER SUMMARY - Part 1*\n📅 {datetime.now().strftime('%B %Y')}\n\n"

        for i, (uid, data) in enumerate(sorted_members):
            member_text = f"👤 *{data['name']}*\n"
            member_text += f"💸 ₹{data['total_spent']:.2f} | 🍽️ {data['meals']}\n"

            if data['balance'] > 0:
                member_text += f"✅ Gets: ₹{data['balance']:.2f}\n\n"
            elif data['balance'] < 0:
                member_text += f"⚠️ Pays: ₹{abs(data['balance']):.2f}\n\n"
            else:
                member_text += f"✔️ Settled\n\n"

            if len(current_chunk) + len(member_text) > chunk_size:
                chunks.append(current_chunk)
                current_chunk = f"👥 *MEMBER SUMMARY - Part {len(chunks) + 1}*\n\n" + member_text
            else:
                current_chunk += member_text

        if current_chunk:
            chunks.append(current_chunk)

        await query.edit_message_text(
            chunks[0],
            parse_mode='Markdown'
        )

        for chunk in chunks[1:]:
            await query.message.reply_text(
                chunk,
                parse_mode='Markdown'
            )

        await query.message.reply_text(
            f"━━━━━━━━━━━━━━\n📊 Total Members: {len(sorted_members)}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

async def show_mystats_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show my stats from button"""
    query = update.callback_query

    stats = manager.get_member_stats(
        update.effective_chat.id,
        update.effective_user.id
    )

    text = f"📊 *Your Stats - {datetime.now().strftime('%B %Y')}*\n\n"
    text += f"👤 {update.effective_user.first_name}\n\n"
    text += f"💰 Total Spent: ₹{stats['spent']:.2f}\n"
    text += f"📝 Expenses Added: {stats['expense_count']}\n"
    text += f"🍽️ Meals Consumed: {stats['meals']}"

    keyboard = [[InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu_main")]]

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def show_settlement_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show settlement from button"""
    query = update.callback_query
    group_id = update.effective_chat.id

    if not manager.is_meal_data_submitted(str(group_id)):
        await query.answer("⚠️ Meal data not submitted yet!", show_alert=True)
        return

    result = manager.calculate_settlement(group_id)

    if not result:
        await query.answer("⚠️ No data available!", show_alert=True)
        return

    text = f"💰 *Settlement*\n\n"
    text += f"💵 Per Meal: ₹{result['cost_per_meal']:.2f}\n\n"

    for s in sorted(result['settlements'], key=lambda x: x['balance'], reverse=True)[:5]:
        text += f"👤 {s['name']}\n"

        if s['balance'] > 0:
            text += f"✅ Gets: ₹{s['balance']:.2f}\n\n"
        elif s['balance'] < 0:
            text += f"⚠️ Pays: ₹{abs(s['balance']):.2f}\n\n"
        else:
            text += f"✔️ Settled\n\n"

    keyboard = [[InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu_main")]]

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def show_viewmeals_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show meals from button"""
    query = update.callback_query
    group_id = str(update.effective_chat.id)

    if not manager.is_meal_data_submitted(group_id):
        await query.answer("⚠️ No meal data yet!", show_alert=True)
        return

    meal_data = manager.get_current_month_meals(group_id)
    members = manager.data[group_id].get('members', {})

    text = f"📊 *Meal Counts - {datetime.now().strftime('%B %Y')}*\n\n"

    total = 0
    for uid, count in list(meal_data.items())[:8]:
        name = members.get(uid, {}).get('name', f'User {uid}')
        text += f"• {name}: {count}\n"
        total += count

    if len(meal_data) > 8:
        remaining = sum(list(meal_data.values())[8:])
        text += f"\n...and more ({len(meal_data) - 8} members)"
        total += remaining

    text += f"\n🍽️ Total: {total} meals"

    keyboard = [[InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu_main")]]

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help command"""
    user_is_admin = await is_admin(update, context)

    await update.message.reply_text(
        "🍽️ *Mess Fund Manager*\n\n"
        "Use the menu buttons below to navigate!",
        reply_markup=get_main_menu_keyboard(user_is_admin),
        parse_mode='Markdown'
    )

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show menu"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text("⚠️ Add me to a group first!")
        return

    user_is_admin = await is_admin(update, context)

    await update.message.reply_text(
        "🍽️ *Menu*\n\n"
        "Choose an option:",
        reply_markup=get_main_menu_keyboard(user_is_admin),
        parse_mode='Markdown'
    )

async def register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Register"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text("⚠️ This command only works in groups!")
        return

    user = update.effective_user
    group_id = update.effective_chat.id

    added = manager.add_member(group_id, user.first_name, user.id, user.username or '')

    user_is_admin = await is_admin(update, context)
    msg = f"✅ Welcome {user.first_name}! You're now registered." if added else f"ℹ️ You're already registered!"

    await update.message.reply_text(
        msg,
        reply_markup=get_main_menu_keyboard(user_is_admin)
    )

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
        await update.message.reply_text('📝 What did you buy? (e.g., vegetables, rice)')
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

    user_is_admin = await is_admin(update, context)

    await update.message.reply_text(
        f"✅ *Expense Recorded!*\n\n"
        f"💸 ₹{amount:.2f}\n"
        f"📝 {description}\n"
        f"👤 {user_name}",
        reply_markup=get_main_menu_keyboard(user_is_admin),
        parse_mode='Markdown'
    )

    context.user_data.clear()
    return ConversationHandler.END

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

    user_is_admin = await is_admin(update, context)

    await update.message.reply_text(
        f"✅ *Expense Added by Admin!*\n\n"
        f"👤 Member: {user_name}\n"
        f"💸 Amount: ₹{amount:.2f}\n"
        f"📝 Item: {description}\n\n"
        f"✨ Added by: {update.effective_user.first_name}",
        reply_markup=get_main_menu_keyboard(user_is_admin),
        parse_mode='Markdown'
    )

    context.user_data.clear()
    return ConversationHandler.END

async def admin_edit_meal_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start admin edit meal from button"""
    query = update.callback_query
    await query.answer()

    if not await is_admin(update, context):
        await query.answer("⚠️ Admin only!", show_alert=True)
        return ConversationHandler.END

    group_id = str(update.effective_chat.id)

    if not manager.is_meal_data_submitted(group_id):
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="menu_admin")]]
        await query.edit_message_text(
            "⚠️ *No meal data submitted yet!*\n\n"
            "Use 'Add Meals' button first to add meal data.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return ConversationHandler.END

    meal_data = manager.get_current_month_meals(group_id)
    members = manager.data[group_id].get('members', {})

    context.user_data['group_id'] = update.effective_chat.id

    keyboard = []
    for uid, count in meal_data.items():
        name = members.get(uid, {}).get('name', f'User {uid}')
        keyboard.append([InlineKeyboardButton(
            f"{name} (current: {count})",
            callback_data=f"editmeal_{uid}"
        )])
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="editmeal_cancel")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        "👑 *Admin: Edit Meal Count*\n\n"
        "Select member to edit:",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

    return EDIT_MEAL_SELECT

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
        await query.edit_message_text(
            "👑 *Admin Menu*\n\n"
            "Select an admin action:",
            reply_markup=get_admin_menu_keyboard(),
            parse_mode='Markdown'
        )
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

        user_is_admin = await is_admin(update, context)

        await update.message.reply_text(
            f"✅ *Meal Count Updated!*\n\n"
            f"👤 Member: {user_name}\n"
            f"🍽️ Old count: {old_count}\n"
            f"🍽️ New count: {new_count}\n\n"
            f"✨ Updated by: {update.effective_user.first_name}",
            reply_markup=get_main_menu_keyboard(user_is_admin),
            parse_mode='Markdown'
        )

        context.user_data.clear()
        return ConversationHandler.END

    except ValueError:
        await update.message.reply_text('❌ Invalid! Enter a valid number (0 or more).')
        return EDIT_MEAL_COUNT

async def add_meals_from_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start meal entry from menu button"""
    query = update.callback_query
    await query.answer()

    if update.effective_chat.type == 'private':
        await query.answer("⚠️ This works in groups only!", show_alert=True)
        return ConversationHandler.END

    group_id = update.effective_chat.id
    user = query.from_user

    context.user_data['group_id'] = group_id
    context.user_data['submitted_by'] = user.first_name
    context.user_data['meal_data_collection'] = {}

    group_data = manager.data[str(group_id)]
    members = group_data.get('members', {})

    if not members:
        await query.answer("⚠️ No members registered yet!", show_alert=True)
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="menu_main")]]
        await query.edit_message_text(
            "⚠️ No members registered!\n\nMembers need to /register first.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ConversationHandler.END

    keyboard = []
    for uid, info in members.items():
        keyboard.append([InlineKeyboardButton(
            f"👤 {info['name']}",
            callback_data=f"mealmember_{uid}"
        )])

    keyboard.append([InlineKeyboardButton("✅ Finish & Save", callback_data="mealmember_finish")])
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="mealmember_cancel")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        f"📊 *Add Meal Counts - {datetime.now().strftime('%B %Y')}*\n\n"
        f"👥 *Click on a member to add their meal count:*\n\n"
        f"✅ = Already added\n"
        f"👤 = Not added yet\n\n"
        f"Click 'Finish & Save' when done!",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

    return MEAL_MEMBER_SELECT

async def add_meals_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add meals with button selection"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text("⚠️ This command only works in groups!")
        return ConversationHandler.END

    group_id = update.effective_chat.id
    user = update.effective_user

    context.user_data['group_id'] = group_id
    context.user_data['submitted_by'] = user.first_name
    context.user_data['meal_data_collection'] = {}

    group_data = manager.data[str(group_id)]
    members = group_data.get('members', {})

    if not members:
        await update.message.reply_text(
            "⚠️ No members registered yet!\n\n"
            "Members need to use /register first."
        )
        return ConversationHandler.END

    keyboard = []
    for uid, info in members.items():
        if uid in context.user_data.get('meal_data_collection', {}):
            keyboard.append([InlineKeyboardButton(
                f"✅ {info['name']} - {context.user_data['meal_data_collection'][uid]} meals",
                callback_data=f"mealmember_{uid}"
            )])
        else:
            keyboard.append([InlineKeyboardButton(
                f"👤 {info['name']}",
                callback_data=f"mealmember_{uid}"
            )])

    keyboard.append([InlineKeyboardButton("✅ Finish & Save", callback_data="mealmember_finish")])
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="mealmember_cancel")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"📊 *Add Meal Counts - {datetime.now().strftime('%B %Y')}*\n\n"
        f"👥 *Click on a member to add their meal count:*\n\n"
        f"✅ = Already added\n"
        f"👤 = Not added yet\n\n"
        f"Click 'Finish & Save' when done!",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

    return MEAL_MEMBER_SELECT

async def meal_member_select_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle member selection for meal count"""
    query = update.callback_query
    await query.answer()

    if query.data == "mealmember_cancel":
        await query.edit_message_text("❌ Cancelled.")
        context.user_data.clear()
        return ConversationHandler.END

    if query.data == "mealmember_finish":
        meal_data = context.user_data.get('meal_data_collection', {})

        if not meal_data:
            await query.answer("⚠️ Please add at least one member's meal count!", show_alert=True)
            return MEAL_MEMBER_SELECT

        group_id = str(context.user_data['group_id'])
        members = manager.data[group_id].get('members', {})

        text = "📋 *Confirm Meal Data*\n\n"
        total_meals = 0
        for uid, count in meal_data.items():
            name = members.get(uid, {}).get('name', f'User {uid}')
            text += f"• {name}: {count} meals\n"
            total_meals += count

        text += f"\n🍽️ *Total Meals: {total_meals}*\n\n"
        text += "*Is this correct?*"

        keyboard = [
            [
                InlineKeyboardButton("✅ Yes, Save", callback_data="mealfinish_yes"),
                InlineKeyboardButton("❌ No, Edit", callback_data="mealfinish_no")
            ]
        ]

        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

        return MEAL_CONFIRM

    user_id = query.data.replace("mealmember_", "")
    group_id = str(context.user_data['group_id'])
    member_name = manager.data[group_id]['members'][user_id]['name']

    context.user_data['current_member_id'] = user_id
    context.user_data['current_member_name'] = member_name

    existing_count = context.user_data['meal_data_collection'].get(user_id, None)

    if existing_count:
        await query.edit_message_text(
            f"👤 *{member_name}*\n\n"
            f"Current meal count: *{existing_count}*\n\n"
            f"Enter new meal count:",
            parse_mode='Markdown'
        )
    else:
        await query.edit_message_text(
            f"👤 *{member_name}*\n\n"
            f"Enter meal count for this member:",
            parse_mode='Markdown'
        )

    return MEAL_COUNT_INPUT

async def meal_count_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle meal count input"""
    try:
        count = int(update.message.text)
        if count < 0:
            raise ValueError

        user_id = context.user_data['current_member_id']
        member_name = context.user_data['current_member_name']

        context.user_data['meal_data_collection'][user_id] = count

        group_id = str(context.user_data['group_id'])
        members = manager.data[group_id].get('members', {})

        keyboard = []
        for uid, info in members.items():
            if uid in context.user_data['meal_data_collection']:
                keyboard.append([InlineKeyboardButton(
                    f"✅ {info['name']} - {context.user_data['meal_data_collection'][uid]} meals",
                    callback_data=f"mealmember_{uid}"
                )])
            else:
                keyboard.append([InlineKeyboardButton(
                    f"👤 {info['name']}",
                    callback_data=f"mealmember_{uid}"
                )])

        keyboard.append([InlineKeyboardButton("✅ Finish & Save", callback_data="mealmember_finish")])
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="mealmember_cancel")])

        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            f"✅ *Added: {member_name} - {count} meals*\n\n"
            f"📊 Progress: {len(context.user_data['meal_data_collection'])}/{len(members)} members\n\n"
            f"Click another member to continue, or 'Finish & Save':",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )

        return MEAL_MEMBER_SELECT

    except ValueError:
        await update.message.reply_text(
            '❌ Invalid number! Please enter a valid meal count (0 or more).'
        )
        return MEAL_COUNT_INPUT

async def meal_finish_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle final confirmation"""
    query = update.callback_query
    await query.answer()

    if query.data == "mealfinish_yes":
        group_id = context.user_data['group_id']
        submitted_by = context.user_data['submitted_by']
        meal_data = context.user_data['meal_data_collection']

        manager.set_meal_counts(group_id, meal_data, submitted_by)

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

        keyboard = [[InlineKeyboardButton("📱 Main Menu", callback_data="menu_main")]]

        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

        context.user_data.clear()
        return ConversationHandler.END

    elif query.data == "mealfinish_no":
        group_id = str(context.user_data['group_id'])
        members = manager.data[group_id].get('members', {})

        keyboard = []
        for uid, info in members.items():
            if uid in context.user_data['meal_data_collection']:
                keyboard.append([InlineKeyboardButton(
                    f"✅ {info['name']} - {context.user_data['meal_data_collection'][uid]} meals",
                    callback_data=f"mealmember_{uid}"
                )])
            else:
                keyboard.append([InlineKeyboardButton(
                    f"👤 {info['name']}",
                    callback_data=f"mealmember_{uid}"
                )])

        keyboard.append([InlineKeyboardButton("✅ Finish & Save", callback_data="mealmember_finish")])
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="mealmember_cancel")])

        await query.edit_message_text(
            f"📊 *Edit Meal Counts*\n\n"
            f"Click on a member to update their count:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

        return MEAL_MEMBER_SELECT

async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Summary command"""
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
        text += f"💵 Cost per Meal: ₹{(total+carry)/total_meals:.2f}\n"

    if not manager.is_meal_data_submitted(group_id):
        text += "\n⚠️ Meal data not submitted yet!"

    user_is_admin = await is_admin(update, context)

    await update.message.reply_text(
        text,
        reply_markup=get_main_menu_keyboard(user_is_admin),
        parse_mode='Markdown'
    )

async def members_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Members command"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text("⚠️ This command only works in groups!")
        return

    group_id = str(update.effective_chat.id)
    member_summary = manager.get_member_wise_summary(group_id)

    if not member_summary:
        await update.message.reply_text("⚠️ No member data available yet!")
        return

    sorted_members = sorted(
        member_summary.items(),
        key=lambda x: x[1]['balance'],
        reverse=True
    )

    text = f"👥 *MEMBER SUMMARY*\n"
    text += f"📅 {datetime.now().strftime('%B %Y')}\n\n"

    for uid, data in sorted_members:
        text += f"👤 *{data['name']}*\n"
        text += f"💸 ₹{data['total_spent']:.2f} | 🍽️ {data['meals']}\n"

        if data['balance'] > 0:
            text += f"✅ Gets: ₹{data['balance']:.2f}\n\n"
        elif data['balance'] < 0:
            text += f"⚠️ Pays: ₹{abs(data['balance']):.2f}\n\n"
        else:
            text += f"✔️ Settled\n\n"

    text += f"━━━━━━━━━━━━━━\n"
    text += f"📊 Total Members: {len(sorted_members)}"

    user_is_admin = await is_admin(update, context)

    if len(text) > 4000:
        chunk_size = 3500
        chunks = []
        current_chunk = f"👥 *MEMBER SUMMARY - Part 1*\n📅 {datetime.now().strftime('%B %Y')}\n\n"

        for uid, data in sorted_members:
            member_text = f"👤 *{data['name']}*\n"
            member_text += f"💸 ₹{data['total_spent']:.2f} | 🍽️ {data['meals']}\n"

            if data['balance'] > 0:
                member_text += f"✅ Gets: ₹{data['balance']:.2f}\n\n"
            elif data['balance'] < 0:
                member_text += f"⚠️ Pays: ₹{abs(data['balance']):.2f}\n\n"
            else:
                member_text += f"✔️ Settled\n\n"

            if len(current_chunk) + len(member_text) > chunk_size:
                chunks.append(current_chunk)
                current_chunk = f"👥 *MEMBER SUMMARY - Part {len(chunks) + 1}*\n\n" + member_text
            else:
                current_chunk += member_text

        if current_chunk:
            chunks.append(current_chunk)

        for chunk in chunks:
            await update.message.reply_text(
                chunk,
                parse_mode='Markdown'
            )

        await update.message.reply_text(
            f"━━━━━━━━━━━━━━\n📊 Total Members: {len(sorted_members)}",
            reply_markup=get_main_menu_keyboard(user_is_admin)
        )
    else:
        await update.message.reply_text(
            text,
            reply_markup=get_main_menu_keyboard(user_is_admin),
            parse_mode='Markdown'
        )

async def settlement(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Settlement command"""
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
        await update.message.reply_text("⚠️ No data available!")
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

        if s['balance'] > 0:
            text += f"   ✅ Gets Back: ₹{s['balance']:.2f}\n\n"
        elif s['balance'] < 0:
            text += f"   ⚠️ Needs to Pay: ₹{abs(s['balance']):.2f}\n\n"
        else:
            text += f"   ✔️ Settled\n\n"

    user_is_admin = await is_admin(update, context)

    await update.message.reply_text(
        text,
        reply_markup=get_main_menu_keyboard(user_is_admin),
        parse_mode='Markdown'
    )

async def reset_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reset month command"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text("⚠️ This command only works in groups!")
        return

    if not await is_admin(update, context):
        await update.message.reply_text("⚠️ Only admins can reset the month!")
        return

    group_id = update.effective_chat.id
    settlement = manager.reset_month(group_id)

    user_is_admin = True

    if settlement:
        await update.message.reply_text(
            f"✅ Month has been archived!\n\n"
            f"Settlement for {settlement['month']} saved.\n"
            f"Balance ₹{settlement.get('remaining', 0):.2f} carried forward.\n\n"
            f"Starting fresh for the new month!",
            reply_markup=get_main_menu_keyboard(user_is_admin)
        )
    else:
        await update.message.reply_text(
            "✅ Month reset! No data to archive.",
            reply_markup=get_main_menu_keyboard(user_is_admin)
        )

async def export_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Export data command"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text("⚠️ This command only works in groups!")
        return

    if not await is_admin(update, context):
        await update.message.reply_text("⚠️ Only admins can export data!")
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

        user_is_admin = True
        await update.message.reply_text(
            "✅ Data exported successfully!",
            reply_markup=get_main_menu_keyboard(user_is_admin)
        )
    except Exception as e:
        logger.error(f"Export error: {e}")
        await update.message.reply_text(f"❌ Export failed: {str(e)}")

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle any text message and show menu or quick expense"""
    if update.effective_chat.type == 'private':
        await update.message.reply_text(
            "👋 Hi! Add me to a group to manage mess funds!",
            parse_mode='Markdown'
        )
        return

    if 'quick_expense_amount' in context.user_data:
        amount = context.user_data['quick_expense_amount']
        group_id = context.user_data['quick_expense_group']
        user_id = context.user_data['quick_expense_user_id']
        user_name = context.user_data['quick_expense_user_name']
        description = update.message.text

        manager.add_expense(group_id, amount, description, user_name, user_id)

        user_is_admin = await is_admin(update, context)

        await update.message.reply_text(
            f"✅ *Expense Recorded!*\n\n"
            f"💸 ₹{amount:.2f}\n"
            f"📝 {description}\n"
            f"👤 {user_name}",
            reply_markup=get_main_menu_keyboard(user_is_admin),
            parse_mode='Markdown'
        )

        context.user_data.clear()
        return

    try:
        amount = float(update.message.text)
        if amount > 0:
            user = update.effective_user
            group_id = update.effective_chat.id

            manager.add_member(group_id, user.first_name, user.id, user.username or '')

            keyboard = [
                [InlineKeyboardButton("💸 Yes, Add Expense", callback_data=f"quick_expense_{amount}")],
                [InlineKeyboardButton("📱 Show Menu", callback_data="menu_main")]
            ]

            await update.message.reply_text(
                f"💰 Add expense of ₹{amount}?",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
    except:
        pass

    user_is_admin = await is_admin(update, context)

    await update.message.reply_text(
        "📱 *Menu*",
        reply_markup=get_main_menu_keyboard(user_is_admin),
        parse_mode='Markdown'
    )

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel"""
    context.user_data.clear()

    user_is_admin = False
    try:
        user_is_admin = await is_admin(update, context)
    except:
        pass

    await update.message.reply_text(
        "❌ Cancelled.",
        reply_markup=get_main_menu_keyboard(user_is_admin)
    )
    return ConversationHandler.END

async def post_init(application: Application):
    """Initialize scheduler"""
    global bot_instance, scheduler

    bot_instance = application.bot

    scheduler = AsyncIOScheduler()
    scheduler.add_job(check_meal_reminder, 'cron', hour=10, minute=0)
    scheduler.add_job(check_month_end, 'cron', hour=23, minute=59)
    scheduler.start()

    logger.info("✅ Scheduler started!")

def main():
    """Main"""
    BOT_TOKEN = '5875408866:AAEzNrmEj3QV7F19TigTISfCkODcwsKqwf8'

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    expense_handler = ConversationHandler(
        entry_points=[CommandHandler('expense', expense_start)],
        states={
            AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, amount_handler)],
            DESCRIPTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, description_handler)],
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    admin_expense_handler = ConversationHandler(
        entry_points=[CommandHandler('addexpense', admin_add_expense_start)],
        states={
            ADMIN_EXPENSE_MEMBER: [CallbackQueryHandler(admin_expense_member_select, pattern='^adminexp_')],
            ADMIN_EXPENSE_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_expense_amount_handler)],
            ADMIN_EXPENSE_DESC: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_expense_desc_handler)],
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    admin_edit_meal_handler = ConversationHandler(
        entry_points=[
            CommandHandler('editmeal', admin_edit_meal_start),
            CallbackQueryHandler(admin_edit_meal_from_button, pattern='^admin_editmeal$')
        ],
        states={
            EDIT_MEAL_SELECT: [CallbackQueryHandler(admin_edit_meal_select, pattern='^editmeal_')],
            EDIT_MEAL_COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_meal_count_handler)],
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    meal_handler = ConversationHandler(
        entry_points=[
            CommandHandler('addmeals', add_meals_start),
            CallbackQueryHandler(add_meals_from_menu, pattern='^menu_addmeals$')
        ],
        states={
            MEAL_MEMBER_SELECT: [CallbackQueryHandler(meal_member_select_handler, pattern='^mealmember_')],
            MEAL_COUNT_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, meal_count_input_handler)],
            MEAL_CONFIRM: [CallbackQueryHandler(meal_finish_callback, pattern='^mealfinish_')]
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('help', help_command))
    application.add_handler(CommandHandler('menu', menu_command))
    application.add_handler(CommandHandler('register', register))
    application.add_handler(expense_handler)
    application.add_handler(admin_expense_handler)
    application.add_handler(admin_edit_meal_handler)
    application.add_handler(meal_handler)
    application.add_handler(CommandHandler('summary', summary))
    application.add_handler(CommandHandler('members', members_command))
    application.add_handler(CommandHandler('settlement', settlement))
    application.add_handler(CommandHandler('reset', reset_month))
    application.add_handler(CommandHandler('export', export_data))

    application.add_handler(CallbackQueryHandler(menu_callback_handler, pattern='^(?!menu_addmeals$|admin_editmeal$)(menu_|admin_|quick_expense_)'))

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

    print('🤖 Mess Fund Manager Bot is running...')
    print('🎯 Button-based interface enabled!')
    print('📅 Auto settlement enabled')
    print('👑 Admin features enabled')
    print('💬 Auto menu on any message!')
    print('⚡ Quick expense feature enabled!')
    print('✨ Button-based meal entry!')
    print('✨ Button-based admin edit meal!')
    print('✨ All systems ready!')

    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()