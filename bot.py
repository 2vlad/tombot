#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sqlite3
import os
import time
from datetime import datetime, timedelta
import pytz
from telegram import Update, ParseMode, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, ConversationHandler

# Настройки бота - можно легко изменять

# Настройки кнопок и сообщений
# Для каждой кнопки можно задать название и текст ответного сообщения

# Первая кнопка (последнее занятие)
BUTTON_LATEST_LESSON = 'Запись занятия 18 мая'
MSG_LATEST_LESSON = '''Запись занятия: https://drive.google.com/drive/folders/12j6-RCss8JyLqWwLV8pd1KidKT_84cYb?usp=drive_link

Запись доступна в течение 7 дней.'''

# Вторая кнопка (предыдущее занятие)
BUTTON_PREVIOUS_LESSON = 'Запись занятия 22 мая'
MSG_PREVIOUS_LESSON = '''Запись занятия: https://drive.google.com/drive/folders/12j6-RCss8JyLqWwLV8pd1KidKT_84cYb?usp=drive_link

Запись доступна в течение 7 дней.'''

# Третья кнопка (обновление клавиатуры)
BUTTON_REFRESH = 'Обновить'

# Словарь для хранения кнопок и сообщений
BUTTONS = {
    1: {'text': BUTTON_LATEST_LESSON, 'message': MSG_LATEST_LESSON},
    2: {'text': BUTTON_PREVIOUS_LESSON, 'message': MSG_PREVIOUS_LESSON},
    3: {'text': BUTTON_REFRESH, 'message': ''}
}

# Общие тексты сообщений
MSG_WELCOME = 'Привет, я бот для занятий по авангардному кино. Чтобы получить запись прошедшего занятия, нажми кнопку. Записи хранятся 7 дней.'
MSG_ACCOUNT_ACTIVATED = 'Аккаунт активирован. Используй кнопки для доступа к записям занятий.'
MSG_NOT_AUTHORIZED = 'Для доступа к боту необходимо зарегистрироваться. Пожалуйста, обратись к @tovlad.'

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# Database setup
def setup_database():
    conn = sqlite3.connect('filmschool.db')
    cursor = conn.cursor()
    
    # Create users table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        is_admin INTEGER DEFAULT 0,
        registration_date TEXT
    )
    ''')
    
    # Create videos table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS videos (
        id INTEGER PRIMARY KEY,
        title TEXT,
        url TEXT,
        upload_date TEXT
    )
    ''')
    
    # Create logs table with additional fields for detailed statistics
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        action TEXT,
        action_data TEXT,
        timestamp TEXT,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
    ''')
    
    # Create pending_users table for users who have contacted the bot but are not yet authorized
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pending_users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        request_date TEXT
    )
    ''')
    
    # Create buttons table to store button texts and messages
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS buttons (
        id INTEGER PRIMARY KEY,
        button_number INTEGER,
        button_text TEXT,
        message_text TEXT,
        last_updated TEXT
    )
    ''')
    
    # Insert default admin if not exists
    # Replace ADMIN_ID with the actual Telegram ID of the admin
    admin_id = os.environ.get('ADMIN_ID', None)
    if admin_id:
        cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (int(admin_id),))
        if not cursor.fetchone():
            now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute("INSERT INTO users (user_id, is_admin, registration_date) VALUES (?, 1, ?)", 
                          (int(admin_id), now))
    
    conn.commit()
    conn.close()

# User authentication
def is_user_authorized(user_id, username=None):
    conn = sqlite3.connect('filmschool.db')
    cursor = conn.cursor()
    
    # Проверяем по ID
    cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    
    # Если не нашли по ID, но есть username, проверяем по нему
    if result is None and username:
        cursor.execute("SELECT user_id FROM users WHERE username = ?", (username,))
        result = cursor.fetchone()
    
    conn.close()
    return result is not None

def is_admin(user_id):
    conn = sqlite3.connect('filmschool.db')
    cursor = conn.cursor()
    cursor.execute("SELECT is_admin FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result and result[0] == 1

# Functions for working with button settings in the database and environment variables
def load_buttons_from_db():
    global BUTTON_LATEST_LESSON, MSG_LATEST_LESSON, BUTTON_PREVIOUS_LESSON, MSG_PREVIOUS_LESSON, BUTTONS
    
    # First, check environment variables
    button1_text = os.environ.get('BUTTON1_TEXT')
    button1_message = os.environ.get('BUTTON1_MESSAGE')
    button2_text = os.environ.get('BUTTON2_TEXT')
    button2_message = os.environ.get('BUTTON2_MESSAGE')
    
    # If environment variables are set, use them
    if button1_text and button1_message:
        BUTTON_LATEST_LESSON = button1_text
        MSG_LATEST_LESSON = button1_message
        BUTTONS[1]['text'] = BUTTON_LATEST_LESSON
        BUTTONS[1]['message'] = MSG_LATEST_LESSON
    
    if button2_text and button2_message:
        BUTTON_PREVIOUS_LESSON = button2_text
        MSG_PREVIOUS_LESSON = button2_message
        BUTTONS[2]['text'] = BUTTON_PREVIOUS_LESSON
        BUTTONS[2]['message'] = MSG_PREVIOUS_LESSON
    
    # Then check the database
    conn = sqlite3.connect('filmschool.db')
    cursor = conn.cursor()
    
    # Check if there are any saved buttons in the database
    cursor.execute("SELECT COUNT(*) FROM buttons")
    count = cursor.fetchone()[0]
    
    if count > 0:
        # Load button 1 (latest lesson)
        cursor.execute("SELECT button_text, message_text FROM buttons WHERE button_number = 1")
        button1_data = cursor.fetchone()
        if button1_data:
            BUTTON_LATEST_LESSON = button1_data[0]
            MSG_LATEST_LESSON = button1_data[1]
            BUTTONS[1]['text'] = BUTTON_LATEST_LESSON
            BUTTONS[1]['message'] = MSG_LATEST_LESSON
        
        # Load button 2 (previous lesson)
        cursor.execute("SELECT button_text, message_text FROM buttons WHERE button_number = 2")
        button2_data = cursor.fetchone()
        if button2_data:
            BUTTON_PREVIOUS_LESSON = button2_data[0]
            MSG_PREVIOUS_LESSON = button2_data[1]
            BUTTONS[2]['text'] = BUTTON_PREVIOUS_LESSON
            BUTTONS[2]['message'] = MSG_PREVIOUS_LESSON
    else:
        # If no buttons in database, initialize with default values
        save_button_to_db(1, BUTTON_LATEST_LESSON, MSG_LATEST_LESSON)
        save_button_to_db(2, BUTTON_PREVIOUS_LESSON, MSG_PREVIOUS_LESSON)
    
    conn.close()

def save_button_to_db(button_number, button_text, message_text):
    conn = sqlite3.connect('filmschool.db')
    cursor = conn.cursor()
    
    # Current timestamp
    now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
    
    # Check if button already exists
    cursor.execute("SELECT id FROM buttons WHERE button_number = ?", (button_number,))
    button_id = cursor.fetchone()
    
    if button_id:
        # Update existing button
        cursor.execute(
            "UPDATE buttons SET button_text = ?, message_text = ?, last_updated = ? WHERE button_number = ?", 
            (button_text, message_text, now, button_number)
        )
    else:
        # Insert new button
        cursor.execute(
            "INSERT INTO buttons (button_number, button_text, message_text, last_updated) VALUES (?, ?, ?, ?)", 
            (button_number, button_text, message_text, now)
        )
    
    conn.commit()
    conn.close()
    
    # Также сохраняем настройки в переменные окружения для Railway
    try:
        # Для Railway мы не можем напрямую установить переменные окружения из кода
        # Но мы можем записать их в файл .env, который можно использовать при локальной разработке
        # и сообщить администратору, что нужно добавить эти переменные в Railway
        env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
        
        # Создаем или обновляем файл .env
        with open(env_file, 'a+') as f:
            f.seek(0)  # Перемещаемся в начало файла для чтения
            lines = f.readlines()
            
            # Создаем словарь существующих переменных
            env_vars = {}
            for line in lines:
                if '=' in line:
                    key, value = line.strip().split('=', 1)
                    env_vars[key] = value
            
            # Обновляем переменные для кнопок
            if button_number == 1:
                env_vars['BUTTON1_TEXT'] = f'"{button_text}"'
                env_vars['BUTTON1_MESSAGE'] = f'"{message_text}"'
            elif button_number == 2:
                env_vars['BUTTON2_TEXT'] = f'"{button_text}"'
                env_vars['BUTTON2_MESSAGE'] = f'"{message_text}"'
            
            # Перезаписываем файл с обновленными переменными
            f.seek(0)
            f.truncate()
            for key, value in env_vars.items():
                f.write(f"{key}={value}\n")
    except Exception as e:
        logger.error(f"Error saving environment variables: {e}")

# Log user actions with detailed information
def log_action(user_id, action, action_data=None):
    conn = sqlite3.connect('filmschool.db')
    cursor = conn.cursor()
    
    # Get user information
    cursor.execute("SELECT username, first_name, last_name FROM users WHERE user_id = ?", (user_id,))
    user_info = cursor.fetchone()
    
    if user_info:
        username, first_name, last_name = user_info
    else:
        username, first_name, last_name = None, None, None
    
    # Current timestamp
    now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
    
    # Insert log with detailed information
    cursor.execute(
        "INSERT INTO logs (user_id, username, first_name, last_name, action, action_data, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)", 
        (user_id, username, first_name, last_name, action, action_data, now)
    )
    
    conn.commit()
    conn.close()

# Command handlers
def start(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    user_id = user.id
    username = user.username
    first_name = user.first_name
    last_name = user.last_name
    
    conn = sqlite3.connect('filmschool.db')
    cursor = conn.cursor()
    
    # Проверяем, есть ли пользователь с таким username, но с временным ID (отрицательным)
    if username:
        cursor.execute("SELECT user_id FROM users WHERE username = ? AND user_id < 0", (username,))
        temp_user = cursor.fetchone()
        
        if temp_user:
            # Нашли пользователя с временным ID, обновляем на реальный ID
            temp_user_id = temp_user[0]
            
            # Обновляем пользователя с временным ID на реальный
            cursor.execute(
                "UPDATE users SET user_id = ?, first_name = ?, last_name = ? WHERE user_id = ?", 
                (user_id, first_name, last_name, temp_user_id)
            )
            conn.commit()
            
            # Теперь пользователь авторизован
            keyboard = [
                [BUTTON_LATEST_LESSON, BUTTON_PREVIOUS_LESSON]
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            update.message.reply_text(
                MSG_ACCOUNT_ACTIVATED.format(first_name),
                reply_markup=reply_markup
            )
            log_action(user_id, 'start_activated', 'account_activation')
            conn.close()
            return
    
    # Стандартная проверка авторизации
    if is_user_authorized(user_id, username) or is_admin(user_id):
        # Обновляем информацию о пользователе
        cursor.execute(
            "UPDATE users SET username = ?, first_name = ?, last_name = ? WHERE user_id = ?", 
            (username, first_name, last_name, user_id)
        )
        conn.commit()
        
        keyboard = [
            [BUTTON_LATEST_LESSON, BUTTON_PREVIOUS_LESSON],
            [BUTTON_REFRESH]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        # Проверяем, является ли пользователь администратором
        welcome_message = MSG_WELCOME.format(first_name)
        
        # Добавляем информацию о правах администратора, если пользователь является администратором
        if is_admin(user_id):
            welcome_message += '''

Вы имеете права администратора. Используйте /help для просмотра доступных команд.'''
        
        update.message.reply_text(
            welcome_message,
            reply_markup=reply_markup
        )
        log_action(user_id, 'start', 'regular_start')
    else:
        # Сохраняем информацию о пользователе для возможного добавления администратором
        # Проверяем, есть ли информация о пользователе в таблице pending_users
        cursor.execute("SELECT user_id FROM pending_users WHERE user_id = ?", (user_id,))
        if not cursor.fetchone():
            # Добавляем пользователя в таблицу ожидающих
            cursor.execute(
                "INSERT OR REPLACE INTO pending_users (user_id, username, first_name, last_name, request_date) VALUES (?, ?, ?, ?, ?)", 
                (user_id, username, first_name, last_name, datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S'))
            )
            conn.commit()
        
        conn.close()
        
        update.message.reply_text(
            f'Привет, {first_name}! Для доступа к боту необходимо быть зарегистрированным студентом. '
            'Пожалуйста, обратитесь к администратору киношколы для регистрации.'
        )

def refresh_keyboard(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    user_id = user.id
    username = user.username
    
    # Проверяем, что пользователь авторизован
    if not (is_user_authorized(user_id, username) or is_admin(user_id)):
        update.message.reply_text(MSG_NOT_AUTHORIZED)
        return
    
    # Создаем клавиатуру с актуальными кнопками
    keyboard = [
        [BUTTON_LATEST_LESSON, BUTTON_PREVIOUS_LESSON],
        [BUTTON_REFRESH]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    # Отправляем сообщение с обновленной клавиатурой
    update.message.reply_text(
        'Данные обновлены.',
        reply_markup=reply_markup
    )
    
    log_action(user_id, 'refresh_keyboard', 'keyboard_updated')

def help_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    user_id = user.id
    username = user.username
    
    if is_user_authorized(user_id, username) or is_admin(user_id):
        help_text = (
            'Доступные команды:\n'
            '/start - Начать работу с ботом\n'
            '/help - Показать это сообщение\n'
            '/refresh - Обновить кнопки\n\n'
            'Используйте кнопки для доступа к записям занятий.'
        )
        
        if is_admin(user_id):
            help_text += (
                '*Команды администратора:*\n'
                '/adduser <user_id> - Добавить пользователя по ID\n'
                '/addusers @user1 @user2 ... - Массовое добавление пользователей по никнеймам\n'
                '/removeuser <user_id> - Удалить пользователя по ID\n'
                '/makeadmin <user_id или @username> - Назначить пользователя администратором\n'
                '/button1 "Текст кнопки" "URL" - Обновить текст и ссылку для кнопки 1\n'
                '/button2 "Текст кнопки" "URL" - Обновить текст и ссылку для кнопки 2\n'
                '/stats - Показать статистику использования бота\n'
                '/users - Показать список пользователей\n'
                '/pending - Показать список ожидающих подтверждения пользователей\n'
            )
        
        update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)
        log_action(user_id, 'help', 'command')
    else:
        update.message.reply_text(MSG_NOT_AUTHORIZED)

# Admin commands
def add_user(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text('У вас нет прав для выполнения этой команды.')
        return
    
    if not context.args:
        update.message.reply_text('Пожалуйста, укажите Telegram ID или @username пользователя.')
        return
    
    user_identifier = context.args[0]
    
    # Проверяем, является ли идентификатор числом (ID) или именем пользователя
    if user_identifier.isdigit():
        # Если это ID
        new_user_id = int(user_identifier)
        username = None
    elif user_identifier.startswith('@'):
        # Если это @username
        username = user_identifier[1:]  # Убираем символ @
        
        # Проверяем, есть ли пользователь с таким именем в базе
        conn = sqlite3.connect('filmschool.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT user_id FROM users WHERE username = ?", (username,))
        existing_user = cursor.fetchone()
        
        if existing_user:
            update.message.reply_text(f'Пользователь @{username} уже зарегистрирован.')
            conn.close()
            return
        
        # Проверяем, есть ли пользователь в таблице pending_users
        cursor.execute("SELECT user_id FROM pending_users WHERE username = ?", (username,))
        pending_user = cursor.fetchone()
        
        if pending_user:
            # Если пользователь уже взаимодействовал с ботом, добавляем его из pending_users
            new_user_id = pending_user[0]
            
            # Получаем полную информацию о пользователе
            cursor.execute("SELECT user_id, username, first_name, last_name, request_date FROM pending_users WHERE user_id = ?", (new_user_id,))
            user_data = cursor.fetchone()
            
            if user_data:
                user_id, username, first_name, last_name, _ = user_data
                now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
                
                # Добавляем пользователя в авторизованные
                cursor.execute("INSERT INTO users (user_id, username, first_name, last_name, registration_date) VALUES (?, ?, ?, ?, ?)", 
                              (user_id, username, first_name, last_name, now))
                
                # Удаляем из pending_users
                cursor.execute("DELETE FROM pending_users WHERE user_id = ?", (user_id,))
                
                conn.commit()
                conn.close()
                
                update.message.reply_text(f'Пользователь @{username} (ID: {user_id}) успешно добавлен.')
                log_action(user_id, 'add_user', f'user_id:{user_id}')
                return
        
        # Если пользователь не найден в pending_users, добавляем его напрямую
        # Создаем временный ID (отрицательное число) - при первом взаимодействии с ботом он будет обновлен
        temp_user_id = -int(time.time())  # Используем текущее время как временный ID
        now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute("INSERT INTO users (user_id, username, registration_date) VALUES (?, ?, ?)", 
                      (temp_user_id, username, now))
        
        conn.commit()
        conn.close()
        
        update.message.reply_text(
            f'Пользователь @{username} добавлен с временным ID. '
            f'ID будет автоматически обновлен, когда пользователь напишет боту /start.'
        )
        log_action(user_id, 'add_user', f'username:@{username}')
        return
    else:
        update.message.reply_text('Пожалуйста, укажите корректный Telegram ID или @username пользователя.')
        return
    
    conn = sqlite3.connect('filmschool.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (new_user_id,))
    if cursor.fetchone():
        update.message.reply_text(f'Пользователь с ID {new_user_id} уже зарегистрирован.')
        conn.close()
        return
    
    now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("INSERT INTO users (user_id, username, registration_date) VALUES (?, ?, ?)", 
                  (new_user_id, username, now))
    conn.commit()
    conn.close()
    
    update.message.reply_text(f'Пользователь с ID {new_user_id} успешно добавлен.')
    log_action(user_id, 'add_user', f'user_id:{new_user_id}')

def add_users(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    
    # Проверяем, что команду выполняет администратор
    if not is_admin(user_id):
        update.message.reply_text('У вас нет прав для выполнения этой команды.')
        return
    
    # Проверяем, что есть текст после команды
    if not context.args and not update.message.text.split(' ', 1)[1:]:
        update.message.reply_text('''Пожалуйста, укажите список никнеймов пользователей для добавления.

Пример: `/addusers @user1 @user2 @user3`
Или отправьте список никнеймов, каждый в новой строке.''', parse_mode=ParseMode.MARKDOWN)
        return
    
    # Получаем текст после команды
    if context.args:
        # Если аргументы переданы через пробелы
        usernames_text = ' '.join(context.args)
    else:
        # Если текст передан после команды
        usernames_text = update.message.text.split(' ', 1)[1]
    
    # Разбиваем текст на строки и пробелы, удаляем пустые строки
    usernames = [username.strip() for username in re.split(r'[\s\n]+', usernames_text) if username.strip()]
    
    # Проверяем формат никнеймов и удаляем @ если есть
    clean_usernames = []
    for username in usernames:
        if username.startswith('@'):
            clean_usernames.append(username[1:])  # Удаляем символ @
        else:
            clean_usernames.append(username)
    
    if not clean_usernames:
        update.message.reply_text('Не удалось найти допустимые никнеймы в вашем списке.')
        return
    
    # Подключаемся к базе данных
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    
    # Статистика добавления
    added_count = 0
    already_exists_count = 0
    not_found_count = 0
    not_found_usernames = []
    
    # Проходим по всем никнеймам
    for username in clean_usernames:
        # Проверяем, есть ли пользователь в таблице pending_users
        cursor.execute("SELECT user_id, username FROM pending_users WHERE username = ?", (username,))
        pending_user = cursor.fetchone()
        
        if pending_user:
            # Пользователь найден в ожидающих
            pending_user_id, pending_username = pending_user
            
            # Проверяем, не существует ли уже такой пользователь в таблице users
            cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (pending_user_id,))
            existing_user = cursor.fetchone()
            
            if not existing_user:
                # Добавляем пользователя в таблицу users
                now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute(
                    "INSERT INTO users (user_id, username, registration_date, is_admin) VALUES (?, ?, ?, ?)", 
                    (pending_user_id, username, now, 0)
                )
                
                # Удаляем из ожидающих
                cursor.execute("DELETE FROM pending_users WHERE user_id = ?", (pending_user_id,))
                
                added_count += 1
                log_action(user_id, 'add_user', f'username:@{username}, user_id:{pending_user_id}')
            else:
                already_exists_count += 1
        else:
            # Пользователь не найден в ожидающих
            not_found_count += 1
            not_found_usernames.append(username)
    
    conn.commit()
    conn.close()
    
    # Формируем отчет
    report = f"Добавлено пользователей: {added_count}"
    report += f"\nУже существуют: {already_exists_count}"
    
    if not_found_count > 0:
        report += f"\n\nНе найдено в списке ожидающих: {not_found_count}"
        if not_found_usernames:
            report += "\nНе найдены: " + ", ".join([f"@{username}" for username in not_found_usernames[:10]])
            if len(not_found_usernames) > 10:
                report += f" и еще {len(not_found_usernames) - 10}"
    
    update.message.reply_text(report)

def remove_user(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text('У вас нет прав для выполнения этой команды.')
        return
    
    if not context.args:
        update.message.reply_text('Пожалуйста, укажите Telegram ID или @username пользователя.')
        return
    
    user_identifier = context.args[0]
    
    conn = sqlite3.connect('filmschool.db')
    cursor = conn.cursor()
    
    # Определяем, является ли идентификатор числом (ID) или именем пользователя
    if user_identifier.isdigit():
        # Если это ID
        remove_user_id = int(user_identifier)
        
        # Проверяем, является ли пользователь администратором
        if is_admin(remove_user_id):
            update.message.reply_text('Невозможно удалить администратора.')
            conn.close()
            return
        
        cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (remove_user_id,))
        if not cursor.fetchone():
            update.message.reply_text(f'Пользователь с ID {remove_user_id} не найден.')
            conn.close()
            return
        
        cursor.execute("DELETE FROM users WHERE user_id = ?", (remove_user_id,))
        conn.commit()
        conn.close()
        
        update.message.reply_text(f'Пользователь с ID {remove_user_id} успешно удален.')
        log_action(user_id, 'remove_user', f'user_id:{remove_user_id}')
    
    elif user_identifier.startswith('@'):
        # Если это @username
        username = user_identifier[1:]  # Убираем символ @
        
        # Находим пользователя по имени
        cursor.execute("SELECT user_id, is_admin FROM users WHERE username = ?", (username,))
        user_data = cursor.fetchone()
        
        if not user_data:
            update.message.reply_text(f'Пользователь @{username} не найден.')
            conn.close()
            return
        
        remove_user_id, is_admin_flag = user_data
        
        # Проверяем, является ли пользователь администратором
        if is_admin_flag == 1:
            update.message.reply_text('Невозможно удалить администратора.')
            conn.close()
            return
        
        cursor.execute("DELETE FROM users WHERE user_id = ?", (remove_user_id,))
        conn.commit()
        conn.close()
        
        update.message.reply_text(f'Пользователь @{username} (ID: {remove_user_id}) успешно удален.')
        log_action(user_id, 'remove_user', f'user_id:{remove_user_id}')
    
    else:
        update.message.reply_text('Пожалуйста, укажите корректный Telegram ID или @username пользователя.')
        conn.close()

def update_button(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text('У вас нет прав для выполнения этой команды.')
        return
    
    # Проверяем, что переданы все необходимые аргументы
    if len(context.args) < 3:
        update.message.reply_text(
            '''Пожалуйста, укажите все необходимые параметры: 

/button<номер> "<текст кнопки>" "<ссылка>"

Например: /button1 "Запись занятия 19 мая" "https://drive.google.com/drive/folders/12j6-RCss8JyLqWwLV8pd1KidKT84cYb?usp=drivelink"'''
        )
        return
    
    try:
        # Извлекаем номер кнопки из команды (например, /button1 -> 1)
        command = update.message.text.split()[0]  # Получаем /button1
        button_num = int(command.replace('/button', ''))
        
        if button_num not in [1, 2]:
            raise ValueError("Номер кнопки должен быть 1 (последнее занятие) или 2 (предыдущее занятие)")
        
        # Получаем текст кнопки и ссылку
        # Аргументы могут содержать пробелы и быть в кавычках, поэтому используем полный текст сообщения
        full_text = update.message.text
        
        # Находим текст в кавычках
        import re
        matches = re.findall(r'"([^"]*)"', full_text)
        
        if len(matches) < 2:
            raise ValueError("Пожалуйста, укажите текст кнопки и ссылку в кавычках")
        
        button_text = matches[0]
        button_url = matches[1]
        
        # Создаем новый текст сообщения с указанной ссылкой
        message_text = f'''Запись занятия: {button_url}

Запись доступна в течение 7 дней.'''
        
        # Обновляем глобальные переменные в зависимости от номера кнопки
        global BUTTON_LATEST_LESSON, MSG_LATEST_LESSON, BUTTON_PREVIOUS_LESSON, MSG_PREVIOUS_LESSON, BUTTONS
        
        if button_num == 1:
            BUTTON_LATEST_LESSON = button_text
            MSG_LATEST_LESSON = message_text
        else:  # button_num == 2
            BUTTON_PREVIOUS_LESSON = button_text
            MSG_PREVIOUS_LESSON = message_text
        
        # Обновляем словарь кнопок
        BUTTONS[button_num] = {'text': button_text, 'message': message_text}
        
        # Сохраняем изменения в базе данных
        save_button_to_db(button_num, button_text, message_text)
        
        # Формируем сообщение об успехе
        success_message = 'Готово. Нажми «Обновить».'
        
        update.message.reply_text(success_message)
        log_action(user_id, 'update_button', f'button_num:{button_num}, text:"{button_text}", url:{button_url}')
        
    except ValueError as e:
        # Формируем понятное сообщение об ошибке
        error_message = f'''❌ Ошибка при обновлении кнопки:

{str(e)}

Пожалуйста, проверьте формат команды:
/button<номер> "<текст кнопки>" "<ссылка>"'''
        update.message.reply_text(error_message)
    except Exception as e:
        # Формируем детальное сообщение о неожиданной ошибке
        unexpected_error = f'''⚠️ Неожиданная ошибка при обновлении кнопки:

{str(e)}

Пожалуйста, свяжитесь с администратором или попробуйте еще раз.'''
        update.message.reply_text(unexpected_error)

def update_video(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text('У вас нет прав для выполнения этой команды.')
        return
    
    if len(context.args) < 3:
        update.message.reply_text(
            'Пожалуйста, укажите все необходимые параметры: '
            '/updatevideo <номер> <название> <ссылка>'
        )
        return
    
    try:
        video_num = int(context.args[0])
        if video_num not in [1, 2]:
            raise ValueError("Номер видео должен быть 1 (последнее) или 2 (предыдущее)")
        
        title = context.args[1]
        url = context.args[2]
        
        conn = sqlite3.connect('filmschool.db')
        cursor = conn.cursor()
        
        # Get current videos
        cursor.execute("SELECT id FROM videos ORDER BY upload_date DESC LIMIT 2")
        videos = cursor.fetchall()
        
        now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
        
        if len(videos) < 2:
            # Less than 2 videos in database, add new ones
            for i in range(2 - len(videos)):
                cursor.execute("INSERT INTO videos (title, url, upload_date) VALUES (?, ?, ?)", 
                               ("Новое занятие", "https://example.com", now))
        
        # Get videos again after possible insertion
        cursor.execute("SELECT id FROM videos ORDER BY upload_date DESC LIMIT 2")
        videos = cursor.fetchall()
        
        # Update the selected video
        video_id = videos[video_num - 1][0]
        cursor.execute("UPDATE videos SET title = ?, url = ?, upload_date = ? WHERE id = ?", 
                       (title, url, now, video_id))
        
        conn.commit()
        conn.close()
        
        update.message.reply_text(f'Ссылка на {"последнее" if video_num == 1 else "предыдущее"} занятие успешно обновлена.')
        log_action(user_id, 'update_video', f'video_num:{video_num}')
        
    except ValueError as e:
        update.message.reply_text(str(e))
    except Exception as e:
        update.message.reply_text(f'Произошла ошибка: {str(e)}')

def show_actions(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text('У вас нет прав для выполнения этой команды.')
        return
    
    conn = sqlite3.connect('filmschool.db')
    cursor = conn.cursor()
    
    # Last 20 actions
    cursor.execute("""
    SELECT l.username, l.user_id, l.action, l.action_data, l.timestamp 
    FROM logs l 
    ORDER BY l.timestamp DESC LIMIT 20
    """)
    recent_actions = cursor.fetchall()
    
    conn.close()
    
    # Format actions list
    actions_text = f'*Последние действия:*\n\n'
    
    for action in recent_actions:
        username, user_id, action_type, action_data, timestamp = action
        user_display = f'@{username}' if username else f'ID: {user_id}'
        action_info = f'{action_type}'
        if action_data:
            # Экранируем специальные символы Markdown
            safe_action_data = action_data.replace('*', '\\*').replace('_', '\\_').replace('`', '\\`')
            action_info += f' ({safe_action_data})'
        actions_text += f'- {user_display}: {action_info} ({timestamp})\n'
    
    update.message.reply_text(actions_text, parse_mode=ParseMode.MARKDOWN)
    log_action(user_id, 'show_actions', 'admin_command')

def show_stats(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text('У вас нет прав для выполнения этой команды.')
        return
    
    conn = sqlite3.connect('filmschool.db')
    cursor = conn.cursor()
    
    # Total users
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]
    
    # Total admins
    cursor.execute("SELECT COUNT(*) FROM users WHERE is_admin = 1")
    total_admins = cursor.fetchone()[0]
    
    # Count users who started the bot
    cursor.execute("""
    SELECT COUNT(DISTINCT user_id) FROM logs 
    WHERE action IN ('start', 'start_activated')
    """)
    started_bot_count = cursor.fetchone()[0]
    
    # Video access count for each button
    cursor.execute("""
    SELECT COUNT(*) FROM logs 
    WHERE action = 'get_latest_video'
    """)
    latest_video_count = cursor.fetchone()[0]
    
    cursor.execute("""
    SELECT COUNT(*) FROM logs 
    WHERE action = 'get_previous_video'
    """)
    previous_video_count = cursor.fetchone()[0]
    
    # Get users who accessed the latest video with all their access timestamps
    cursor.execute("""
    SELECT username, first_name, last_name, GROUP_CONCAT(timestamp, ', ') as access_times
    FROM logs 
    WHERE action = 'get_latest_video' AND username IS NOT NULL
    GROUP BY username
    ORDER BY MAX(timestamp) DESC
    """)
    latest_video_users = cursor.fetchall()
    
    # Get count of unique users who accessed the latest video
    cursor.execute("""
    SELECT COUNT(DISTINCT username) 
    FROM logs 
    WHERE action = 'get_latest_video' AND username IS NOT NULL
    """)
    latest_video_unique_users = cursor.fetchone()[0]
    
    # Get users who accessed the previous video with all their access timestamps
    cursor.execute("""
    SELECT username, first_name, last_name, GROUP_CONCAT(timestamp, ', ') as access_times
    FROM logs 
    WHERE action = 'get_previous_video' AND username IS NOT NULL
    GROUP BY username
    ORDER BY MAX(timestamp) DESC
    """)
    previous_video_users = cursor.fetchall()
    
    # Get count of unique users who accessed the previous video
    cursor.execute("""
    SELECT COUNT(DISTINCT username) 
    FROM logs 
    WHERE action = 'get_previous_video' AND username IS NOT NULL
    """)
    previous_video_unique_users = cursor.fetchone()[0]
    
    conn.close()
    
    # Format basic statistics
    stats_text = (
        f'*Статистика бота*\n\n'
        f'Всего пользователей: {total_users}\n'
        f'Запустили бота: {started_bot_count}\n'
        f'Администраторов: {total_admins}\n\n'
    )
    
    # Add detailed statistics for the latest video
    stats_text += f'*Запись занятия 18 мая получили ({latest_video_unique_users}):*\n'
    if latest_video_users:
        for user in latest_video_users:
            username, first_name, last_name, access_times = user
            user_display = f'@{username}' if username else ''
            if first_name or last_name:
                name = f'{first_name or ""} {last_name or ""}' .strip()
                if user_display:
                    user_display += f' ({name})'
                else:
                    user_display = name
            
            # Format access times to show all timestamps when user accessed the video
            timestamps = access_times.split(', ')
            if len(timestamps) > 1:
                # Ограничиваем количество отображаемых временных меток для избежания проблем с Markdown
                if len(timestamps) > 3:
                    time_display = f'({timestamps[0]}, {timestamps[1]}, ... и еще {len(timestamps)-2})'
                else:
                    time_display = f'({timestamps[0]}'
                    for ts in timestamps[1:]:
                        time_display += f', {ts}'
                    time_display += ')'
            else:
                time_display = f'({access_times})'
                
            stats_text += f'- {user_display} {time_display}\n'
    else:
        stats_text += '- Никто еще не запрашивал эту запись\n'
    
    # Add detailed statistics for the previous video
    stats_text += f'\n*Запись занятия 22 мая получили ({previous_video_unique_users}):*\n'
    if previous_video_users:
        for user in previous_video_users:
            username, first_name, last_name, access_times = user
            user_display = f'@{username}' if username else ''
            if first_name or last_name:
                name = f'{first_name or ""} {last_name or ""}' .strip()
                if user_display:
                    user_display += f' ({name})'
                else:
                    user_display = name
            
            # Format access times to show all timestamps when user accessed the video
            timestamps = access_times.split(', ')
            if len(timestamps) > 1:
                # Ограничиваем количество отображаемых временных меток для избежания проблем с Markdown
                if len(timestamps) > 3:
                    time_display = f'({timestamps[0]}, {timestamps[1]}, ... и еще {len(timestamps)-2})'
                else:
                    time_display = f'({timestamps[0]}'
                    for ts in timestamps[1:]:
                        time_display += f', {ts}'
                    time_display += ')'
            else:
                time_display = f'({access_times})'
                
            stats_text += f'- {user_display} {time_display}\n'
    else:
        stats_text += '- Никто еще не запрашивал эту запись\n'
    
    # Убрали последние действия в отдельную команду /actions
    
    update.message.reply_text(stats_text, parse_mode=ParseMode.MARKDOWN)
    log_action(user_id, 'show_stats', 'admin_command')

# Video access handlers
def get_latest_video(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    user_id = user.id
    username = user.username
    
    if not is_user_authorized(user_id, username):
        update.message.reply_text(MSG_NOT_AUTHORIZED)
        return
    
    conn = sqlite3.connect('filmschool.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT title, url, upload_date FROM videos ORDER BY upload_date DESC LIMIT 1")
    video = cursor.fetchone()
    conn.close()
    
    if video:
        title, url, date = video
        update.message.reply_text(
            f'*{title}*\n\n'
            f'Дата загрузки: {date}\n\n'
            f'Ссылка: {url}',
            parse_mode=ParseMode.MARKDOWN
        )
        log_action(user_id, 'get_latest_video', 'database_access')
    else:
        update.message.reply_text('Записи занятий пока не добавлены. Пожалуйста, попробуйте позже.')

def get_previous_video(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    user_id = user.id
    username = user.username
    
    if not is_user_authorized(user_id, username):
        update.message.reply_text(MSG_NOT_AUTHORIZED)
        return
    
    conn = sqlite3.connect('filmschool.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT title, url, upload_date FROM videos ORDER BY upload_date DESC LIMIT 2")
    videos = cursor.fetchall()
    conn.close()
    
    if len(videos) >= 2:
        title, url, date = videos[1]  # Second video is the previous one
        update.message.reply_text(
            f'*{title}*\n\n'
            f'Дата загрузки: {date}\n\n'
            f'Ссылка: {url}',
            parse_mode=ParseMode.MARKDOWN
        )
        log_action(user_id, 'get_previous_video', 'database_access')
    else:
        update.message.reply_text('Предыдущее занятие пока не доступно. Пожалуйста, попробуйте позже.')

# Message handler
def handle_message(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    user_id = user.id
    username = user.username
    
    # Разрешаем доступ к кнопкам как авторизованным пользователям, так и администраторам
    if not (is_user_authorized(user_id, username) or is_admin(user_id)):
        update.message.reply_text(MSG_NOT_AUTHORIZED)
        return
    
    text = update.message.text
    
    # Используем словарь BUTTONS для обработки нажатий на кнопки
    if text == BUTTONS[1]['text']:
        # Используем индивидуальный текст сообщения для этой кнопки
        update.message.reply_text(BUTTONS[1]['message'], parse_mode=ParseMode.MARKDOWN)
        # Расширенное логирование с данными о кнопке
        log_action(user_id, 'get_latest_video', BUTTONS[1]['text'])
    elif text == BUTTONS[2]['text']:
        # Используем индивидуальный текст сообщения для этой кнопки
        update.message.reply_text(BUTTONS[2]['message'], parse_mode=ParseMode.MARKDOWN)
        # Расширенное логирование с данными о кнопке
        log_action(user_id, 'get_previous_video', BUTTONS[2]['text'])
    elif text == BUTTONS[3]['text']:
        # Если нажата кнопка "Обновить", вызываем функцию refresh_keyboard
        refresh_keyboard(update, context)
        log_action(user_id, 'refresh_keyboard_button', 'button_click')
    else:
        update.message.reply_text(
            'Пожалуйста, используйте кнопки для доступа к записям занятий.'
        )

def list_users(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text('У вас нет прав для выполнения этой команды.')
        return
    
    conn = sqlite3.connect('filmschool.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT user_id, username, first_name, last_name, is_admin FROM users ORDER BY is_admin DESC, registration_date DESC")
    users = cursor.fetchall()
    conn.close()
    
    if not users:
        update.message.reply_text('В системе нет зарегистрированных пользователей.')
        return
    
    message = "*Список пользователей:*\n\n"
    
    for user in users:
        user_id, username, first_name, last_name, is_admin = user
        user_info = f"ID: `{user_id}`\n"
        
        if username:
            user_info += f"Username: @{username}\n"
        
        name = ""
        if first_name:
            name += first_name
        if last_name:
            name += f" {last_name}"
        
        if name:
            user_info += f"Имя: {name}\n"
        
        if is_admin:
            user_info += "*Администратор*\n"
        
        message += f"{user_info}\n"
    
    update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
    log_action(user_id, 'list_users', 'admin_command')

def pending_users(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text('У вас нет прав для выполнения этой команды.')
        return
    
    conn = sqlite3.connect('filmschool.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT user_id, username, first_name, last_name, request_date FROM pending_users ORDER BY request_date DESC")
    users = cursor.fetchall()
    conn.close()
    
    if not users:
        update.message.reply_text('Нет пользователей, ожидающих регистрации.')
        return
    
    message = "*Пользователи, запросившие доступ:*\n\n"
    
    for user in users:
        user_id, username, first_name, last_name, request_date = user
        user_info = f"ID: `{user_id}`\n"
        
        if username:
            user_info += f"Username: @{username}\n"
        
        name = ""
        if first_name:
            name += first_name
        if last_name:
            name += f" {last_name}"
        
        if name:
            user_info += f"Имя: {name}\n"
        
        user_info += f"Дата запроса: {request_date}\n"
        user_info += f"Добавить: `/adduser {user_id}`\n"
        
        message += f"{user_info}\n"
    
    update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
    log_action(user_id, 'pending_users', 'admin_command')

def make_admin(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    
    # Проверяем, что команду выполняет администратор
    if not is_admin(user_id):
        update.message.reply_text('У вас нет прав для выполнения этой команды.')
        return
    
    # Проверяем, что указан пользователь для повышения до администратора
    if not context.args:
        update.message.reply_text('Пожалуйста, укажите Telegram ID или @username пользователя, которого вы хотите сделать администратором.')
        return
    
    user_identifier = context.args[0]
    conn = sqlite3.connect('filmschool.db')
    cursor = conn.cursor()
    
    # Определяем, является ли идентификатор числом (ID) или именем пользователя
    if user_identifier.isdigit():
        # Если это ID
        target_user_id = int(user_identifier)
        
        # Проверяем, существует ли пользователь с таким ID
        cursor.execute("SELECT user_id, username, is_admin FROM users WHERE user_id = ?", (target_user_id,))
        user_data = cursor.fetchone()
        
        if not user_data:
            update.message.reply_text(f'Пользователь с ID {target_user_id} не найден.')
            conn.close()
            return
            
        user_id, username, is_admin_flag = user_data
        
        # Проверяем, не является ли пользователь уже администратором
        if is_admin_flag == 1:
            update.message.reply_text(f'Пользователь с ID {target_user_id} уже является администратором.')
            conn.close()
            return
        
        # Делаем пользователя администратором
        cursor.execute("UPDATE users SET is_admin = 1 WHERE user_id = ?", (target_user_id,))
        conn.commit()
        
        username_str = f'@{username}' if username else ''
        
        # Создаем сообщение о назначении администратором
        admin_message = f'''✅ Пользователь {username_str} (ID: {target_user_id}) успешно назначен администратором.

ℹ️ Для получения кнопок пользователь должен выполнить команду /start'''
        
        update.message.reply_text(admin_message)
        log_action(user_id, 'make_admin', f'target_user_id:{target_user_id}')
        
    elif user_identifier.startswith('@'):
        # Если это @username
        username = user_identifier[1:]  # Убираем символ @
        
        # Проверяем, существует ли пользователь с таким username
        cursor.execute("SELECT user_id, is_admin FROM users WHERE username = ?", (username,))
        user_data = cursor.fetchone()
        
        if not user_data:
            update.message.reply_text(f'Пользователь @{username} не найден.')
            conn.close()
            return
            
        target_user_id, is_admin_flag = user_data
        
        # Проверяем, не является ли пользователь уже администратором
        if is_admin_flag == 1:
            update.message.reply_text(f'Пользователь @{username} уже является администратором.')
            conn.close()
            return
        
        # Делаем пользователя администратором
        cursor.execute("UPDATE users SET is_admin = 1 WHERE user_id = ?", (target_user_id,))
        conn.commit()
        
        # Создаем сообщение о назначении администратором
        admin_message = f'''✅ Пользователь @{username} (ID: {target_user_id}) успешно назначен администратором.

ℹ️ Для получения кнопок пользователь должен выполнить команду /start'''
        
        update.message.reply_text(admin_message)
        log_action(user_id, 'make_admin', f'target_username:@{username}')
        
    else:
        update.message.reply_text('Пожалуйста, укажите корректный Telegram ID или @username пользователя.')
    
    conn.close()

def main() -> None:
    # Setup database
    setup_database()
    
    # Load button settings from database
    load_buttons_from_db()
    
    # Get token from environment variable
    token = os.environ.get('TELEGRAM_TOKEN')
    if not token:
        logger.error("No token provided. Set the TELEGRAM_TOKEN environment variable.")
        return
    
    # Create the Updater
    updater = Updater(token)
    
    # Get the dispatcher to register handlers
    dispatcher = updater.dispatcher
    
    # Register command handlers
    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(CommandHandler("help", help_command))
    dispatcher.add_handler(CommandHandler("refresh", refresh_keyboard))
    dispatcher.add_handler(CommandHandler("adduser", add_user))
    dispatcher.add_handler(CommandHandler("addusers", add_users))
    dispatcher.add_handler(CommandHandler("removeuser", remove_user))
    dispatcher.add_handler(CommandHandler("updatevideo", update_video))
    dispatcher.add_handler(CommandHandler("stats", show_stats))
    dispatcher.add_handler(CommandHandler("actions", show_actions))
    dispatcher.add_handler(CommandHandler("listusers", list_users))
    dispatcher.add_handler(CommandHandler("pendingusers", pending_users))
    dispatcher.add_handler(CommandHandler("makeadmin", make_admin))
    
    # Register button update command handlers
    dispatcher.add_handler(CommandHandler("button1", update_button))
    dispatcher.add_handler(CommandHandler("button2", update_button))
    
    # Register message handler
    dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))
    
    # Start the Bot
    updater.start_polling()
    logger.info("Bot started")
    
    # Run the bot until you press Ctrl-C
    updater.idle()

if __name__ == '__main__':
    main()
