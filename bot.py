#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sqlite3
import os
import time
import re
import random
import signal
import sys
from datetime import datetime, timedelta
import pytz
from telegram import Update, ParseMode, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, ConversationHandler

# Импортируем модуль для работы с базой данных
from db_utils import setup_database, get_db_connection, load_buttons, save_button

# Импортируем функцию инициализации базы данных
try:
    from init_db import init_database
except ImportError:
    print("Модуль init_db.py не найден, инициализация базы данных не будет выполнена")

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
MSG_NOT_AUTHORIZED = 'Чтобы получить доступ, напиши @tovlad.'

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# Функция setup_database теперь в модуле db_utils
# Администратор также добавляется в модуле db_utils

# User authentication
def is_user_authorized(user_id, username=None, phone_number=None):
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    # Проверяем по ID
    if db_type == 'postgres':
        cursor.execute("SELECT user_id FROM users WHERE user_id = %s", (user_id,))
    else:
        cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    
    # Если не нашли по ID, но есть username, проверяем по нему
    if result is None and username:
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM users WHERE username = %s", (username,))
        else:
            cursor.execute("SELECT user_id FROM users WHERE username = ?", (username,))
        result = cursor.fetchone()
    
    # Если не нашли по ID и username, но есть phone_number, проверяем по нему
    if result is None and phone_number:
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM users WHERE phone_number = %s", (phone_number,))
        else:
            cursor.execute("SELECT user_id FROM users WHERE phone_number = ?", (phone_number,))
        result = cursor.fetchone()
    
    conn.close()
    return result is not None

def is_admin(user_id):
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    if db_type == 'postgres':
        cursor.execute("SELECT is_admin FROM users WHERE user_id = %s", (user_id,))
    else:
        cursor.execute("SELECT is_admin FROM users WHERE user_id = ?", (user_id,))
    
    result = cursor.fetchone()
    conn.close()
    
    if db_type == 'postgres':
        return result and result[0] is True
    else:
        return result and result[0] == 1

# Функция загрузки настроек кнопок
def load_buttons_from_db():
    global BUTTON_LATEST_LESSON, MSG_LATEST_LESSON, BUTTON_PREVIOUS_LESSON, MSG_PREVIOUS_LESSON, BUTTONS
    
    # Сначала проверяем переменные окружения
    button1_text = os.environ.get('BUTTON1_TEXT')
    button1_message = os.environ.get('BUTTON1_MESSAGE')
    button2_text = os.environ.get('BUTTON2_TEXT')
    button2_message = os.environ.get('BUTTON2_MESSAGE')
    
    # Если переменные окружения установлены, используем их
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
    
    # Затем загружаем настройки из базы данных
    buttons_data = load_buttons()
    
    # Обновляем глобальные переменные из загруженных данных
    if 1 in buttons_data:
        BUTTON_LATEST_LESSON = buttons_data[1]['text']
        MSG_LATEST_LESSON = buttons_data[1]['message']
        BUTTONS[1]['text'] = BUTTON_LATEST_LESSON
        BUTTONS[1]['message'] = MSG_LATEST_LESSON
    
    if 2 in buttons_data:
        BUTTON_PREVIOUS_LESSON = buttons_data[2]['text']
        MSG_PREVIOUS_LESSON = buttons_data[2]['message']
        BUTTONS[2]['text'] = BUTTON_PREVIOUS_LESSON
        BUTTONS[2]['message'] = MSG_PREVIOUS_LESSON
    
    # Если нет данных в базе, сохраняем значения по умолчанию
    if not buttons_data:
        save_button_to_db(1, BUTTON_LATEST_LESSON, MSG_LATEST_LESSON)
        save_button_to_db(2, BUTTON_PREVIOUS_LESSON, MSG_PREVIOUS_LESSON)

def save_button_to_db(button_number, button_text, message_text):
    # Используем функцию из модуля db_utils для сохранения настроек кнопок
    save_button(button_number, button_text, message_text)
    
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
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    # Get user information
    if db_type == 'postgres':
        cursor.execute("SELECT username, first_name, last_name FROM users WHERE user_id = %s", (user_id,))
    else:
        cursor.execute("SELECT username, first_name, last_name FROM users WHERE user_id = ?", (user_id,))
    
    user_info = cursor.fetchone()
    
    if user_info:
        username, first_name, last_name = user_info
    else:
        username, first_name, last_name = None, None, None
    
    # Current timestamp
    now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
    
    # Insert log with detailed information
    if db_type == 'postgres':
        cursor.execute(
            "INSERT INTO logs (user_id, username, first_name, last_name, action, action_data, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s)", 
            (user_id, username, first_name, last_name, action, action_data, now)
        )
    else:
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
    
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    # Проверяем, есть ли пользователь с таким username, но с временным ID (отрицательным)
    if username:
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM users WHERE username = %s AND user_id < 0", (username,))
        else:
            cursor.execute("SELECT user_id FROM users WHERE username = ? AND user_id < 0", (username,))
        
        temp_user = cursor.fetchone()
        
        if temp_user:
            # Нашли пользователя с временным ID, обновляем на реальный ID
            temp_user_id = temp_user[0]
            
            # Обновляем пользователя с временным ID на реальный
            if db_type == 'postgres':
                cursor.execute(
                    "UPDATE users SET user_id = %s, first_name = %s, last_name = %s WHERE user_id = %s", 
                    (user_id, first_name, last_name, temp_user_id)
                )
            else:
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
        if db_type == 'postgres':
            cursor.execute(
                "UPDATE users SET username = %s, first_name = %s, last_name = %s WHERE user_id = %s", 
                (username, first_name, last_name, user_id)
            )
        else:
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
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM pending_users WHERE user_id = %s", (user_id,))
        else:
            cursor.execute("SELECT user_id FROM pending_users WHERE user_id = ?", (user_id,))
            
        if not cursor.fetchone():
            # Добавляем пользователя в таблицу ожидающих
            now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
            
            if db_type == 'postgres':
                cursor.execute(
                    "INSERT INTO pending_users (user_id, username, first_name, last_name, request_date) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET username = %s, first_name = %s, last_name = %s, request_date = %s", 
                    (user_id, username, first_name, last_name, now, username, first_name, last_name, now)
                )
            else:
                cursor.execute(
                    "INSERT OR REPLACE INTO pending_users (user_id, username, first_name, last_name, request_date) VALUES (?, ?, ?, ?, ?)", 
                    (user_id, username, first_name, last_name, now)
                )
            conn.commit()
        
        conn.close()
        
        update.message.reply_text(
            f'Привет, {first_name}! {MSG_NOT_AUTHORIZED}'
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
                '/checkusers @user1 @user2 ... - Проверить наличие пользователей в базе данных\n'
                '/diagnosedb - Диагностика базы данных\n'
                '/initdb - Инициализация базы данных (создание таблиц)\n'
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
        update.message.reply_text('Пожалуйста, укажите Telegram ID, @username или номер телефона пользователя.')
        return
    
    user_identifier = context.args[0]
    
    # Добавляем отладочный вывод
    update.message.reply_text(f'Получен идентификатор: {user_identifier}')
    
    # Проверяем, является ли идентификатор числом (ID), именем пользователя или номером телефона
    if user_identifier.isdigit() and len(user_identifier) < 10:
        # Если это ID
        new_user_id = int(user_identifier)
        username = None
    elif user_identifier.startswith('@'):
        # Если это @username
        username = user_identifier[1:]  # Убираем символ @
        
        # Проверяем, есть ли пользователь с таким именем в базе
        conn, db_type = get_db_connection()
        cursor = conn.cursor()
        
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM users WHERE username = %s", (username,))
        else:
            cursor.execute("SELECT user_id FROM users WHERE username = ?", (username,))
            
        existing_user = cursor.fetchone()
        
        if existing_user:
            update.message.reply_text(f'Пользователь @{username} уже зарегистрирован.')
            conn.close()
            return
        
        # Проверяем, есть ли пользователь в таблице pending_users
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(%s) OR LOWER(username) = LOWER('@' || %s)", (username, username))
        else:
            cursor.execute("SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(?) OR LOWER(username) = LOWER('@' || ?)", (username, username))
            
        pending_user = cursor.fetchone()
        
        if pending_user:
            # Если пользователь уже взаимодействовал с ботом, добавляем его из pending_users
            new_user_id = pending_user[0]
            
            # Получаем полную информацию о пользователе
            if db_type == 'postgres':
                cursor.execute("SELECT user_id, username, first_name, last_name, request_date FROM pending_users WHERE user_id = %s", (new_user_id,))
            else:
                cursor.execute("SELECT user_id, username, first_name, last_name, request_date FROM pending_users WHERE user_id = ?", (new_user_id,))
                
            user_data = cursor.fetchone()
            
            if user_data:
                user_id, username, first_name, last_name, _ = user_data
                now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
                
                # Добавляем пользователя в авторизованные
                if db_type == 'postgres':
                    cursor.execute(
                        "INSERT INTO users (user_id, username, first_name, last_name, registration_date) VALUES (%s, %s, %s, %s, %s)", 
                        (user_id, username, first_name, last_name, now)
                    )
                    
                    # Удаляем из ожидающих
                    cursor.execute("DELETE FROM pending_users WHERE user_id = %s", (user_id,))
                else:
                    cursor.execute(
                        "INSERT INTO users (user_id, username, first_name, last_name, registration_date) VALUES (?, ?, ?, ?, ?)", 
                        (user_id, username, first_name, last_name, now)
                    )
                    
                    # Удаляем из ожидающих
                    cursor.execute("DELETE FROM pending_users WHERE user_id = ?", (user_id,))
                
                conn.commit()
                conn.close()
                
                update.message.reply_text(f'Пользователь @{username} (ID: {user_id}) успешно добавлен.')
                log_action(user_id, 'add_user', f'username:@{username}, user_id:{user_id}')
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
    elif user_identifier.startswith('+') or (user_identifier.isdigit() and len(user_identifier) >= 10):
        # Если это номер телефона (начинается с + или это число длиной от 10 цифр)
        # Если номер не начинается с +, добавляем его
        if not user_identifier.startswith('+'):
            phone_number = '+' + user_identifier
        else:
            phone_number = user_identifier
            
        username = None
        
        # Добавляем отладочный вывод
        update.message.reply_text(f'Обрабатываю номер телефона: {phone_number}')
        
        # Проверяем, есть ли пользователь с таким номером телефона в базе
        conn, db_type = get_db_connection()
        cursor = conn.cursor()
        
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM users WHERE phone_number = %s", (phone_number,))
        else:
            cursor.execute("SELECT user_id FROM users WHERE phone_number = ?", (phone_number,))
            
        existing_user = cursor.fetchone()
        
        if existing_user:
            update.message.reply_text(f'Пользователь с номером {phone_number} уже зарегистрирован.')
            conn.close()
            return
        
        # Проверяем, есть ли пользователь в таблице pending_users
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM pending_users WHERE phone_number = %s", (phone_number,))
        else:
            cursor.execute("SELECT user_id FROM pending_users WHERE phone_number = ?", (phone_number,))
            
        pending_user = cursor.fetchone()
        
        if pending_user:
            # Если пользователь уже взаимодействовал с ботом, добавляем его из pending_users
            new_user_id = pending_user[0]
            
            # Получаем полную информацию о пользователе
            if db_type == 'postgres':
                cursor.execute("SELECT user_id, username, first_name, last_name, phone_number, request_date FROM pending_users WHERE user_id = %s", (new_user_id,))
            else:
                cursor.execute("SELECT user_id, username, first_name, last_name, phone_number, request_date FROM pending_users WHERE user_id = ?", (new_user_id,))
                
            user_data = cursor.fetchone()
            
            if user_data:
                user_id_data, username_data, first_name, last_name, phone_number_data, _ = user_data
                now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
                
                # Добавляем пользователя в авторизованные
                if db_type == 'postgres':
                    cursor.execute("INSERT INTO users (user_id, username, first_name, last_name, phone_number, registration_date) VALUES (%s, %s, %s, %s, %s, %s)", 
                                   (user_id_data, username_data, first_name, last_name, phone_number_data, now))
                    
                    # Удаляем из ожидающих
                    cursor.execute("DELETE FROM pending_users WHERE user_id = %s", (user_id_data,))
                else:
                    cursor.execute("INSERT INTO users (user_id, username, first_name, last_name, phone_number, registration_date) VALUES (?, ?, ?, ?, ?, ?)", 
                                   (user_id_data, username_data, first_name, last_name, phone_number_data, now))
                    
                    # Удаляем из pending_users
                    cursor.execute("DELETE FROM pending_users WHERE user_id = ?", (user_id_data,))
                
                conn.commit()
                conn.close()
                
                update.message.reply_text(f'Пользователь с номером {phone_number} (ID: {user_id_data}) успешно добавлен.')
                log_action(user_id, 'add_user', f'phone_number:{phone_number}, user_id:{user_id_data}')
                return
        
        # Если пользователь не найден в pending_users, добавляем его напрямую
        # Создаем временный ID (отрицательное число) - при первом взаимодействии с ботом он будет обновлен
        temp_user_id = -int(time.time())  # Используем текущее время как временный ID
        now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
        
        if db_type == 'postgres':
            cursor.execute("INSERT INTO users (user_id, phone_number, registration_date) VALUES (%s, %s, %s)", 
                          (temp_user_id, phone_number, now))
        else:
            cursor.execute("INSERT INTO users (user_id, phone_number, registration_date) VALUES (?, ?, ?)", 
                          (temp_user_id, phone_number, now))
        
        conn.commit()
        conn.close()
        
        update.message.reply_text(f'Пользователь с номером {phone_number} успешно добавлен.')
        log_action(user_id, 'add_user', f'phone_number:{phone_number}')
        return
    else:
        update.message.reply_text('Пожалуйста, укажите корректный Telegram ID, @username или номер телефона пользователя.')
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
            clean_usernames.append(username[1:].lower())  # Удаляем символ @ и приводим к нижнему регистру
        else:
            clean_usernames.append(username.lower())  # Приводим к нижнему регистру
    
    if not clean_usernames:
        update.message.reply_text('Не удалось найти допустимые никнеймы в вашем списке.')
        return
    
    # Подключаемся к базе данных
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    # Статистика добавления
    added_count = 0
    already_exists_count = 0
    not_found_count = 0
    not_found_usernames = []
    
    # Проходим по всем никнеймам
    for username in clean_usernames:
        # Сначала проверяем, есть ли пользователь уже в таблице users по имени пользователя
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(%s) OR LOWER(username) = LOWER('@' || %s)", (username, username))
        else:
            cursor.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(?) OR LOWER(username) = LOWER('@' || ?)", (username, username))
            
        existing_user_by_name = cursor.fetchone()
        
        if existing_user_by_name:
            # Пользователь уже существует в таблице users
            already_exists_count += 1
            print(f"Пользователь @{username} уже существует в таблице users")
            continue
        
        # Проверяем, есть ли пользователь в таблице pending_users
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(%s) OR LOWER(username) = LOWER('@' || %s)", (username, username))
        else:
            cursor.execute("SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(?) OR LOWER(username) = LOWER('@' || ?)", (username, username))
            
        pending_user = cursor.fetchone()
        
        if pending_user:
            # Пользователь найден в ожидающих
            pending_user_id = pending_user[0]
            
            # Проверяем, не существует ли уже такой пользователь в таблице users по ID
            if db_type == 'postgres':
                cursor.execute("SELECT user_id FROM users WHERE user_id = %s", (pending_user_id,))
            else:
                cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (pending_user_id,))
                
            existing_user = cursor.fetchone()
            
            if not existing_user:
                # Добавляем пользователя в таблицу users
                now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
                
                if db_type == 'postgres':
                    cursor.execute(
                        "INSERT INTO users (user_id, username, registration_date, is_admin) VALUES (%s, %s, %s, %s)", 
                        (pending_user_id, username, now, False)
                    )
                    
                    # Удаляем из ожидающих
                    cursor.execute("DELETE FROM pending_users WHERE user_id = %s", (pending_user_id,))
                else:
                    cursor.execute(
                        "INSERT INTO users (user_id, username, registration_date, is_admin) VALUES (?, ?, ?, ?)", 
                        (pending_user_id, username, now, 0)
                    )
                    
                    # Удаляем из ожидающих
                    cursor.execute("DELETE FROM pending_users WHERE user_id = ?", (pending_user_id,))
                
                added_count += 1
                print(f"Добавлен пользователь @{username} из pending_users с ID {pending_user_id}")
                log_action(user_id, 'add_user', f'username:@{username}, user_id:{pending_user_id}')
            else:
                already_exists_count += 1
                print(f"Пользователь @{username} с ID {pending_user_id} уже существует в таблице users")
        else:
            # Пользователь не найден в ожидающих, добавляем его напрямую
            # Создаем временный ID (отрицательное число) - при первом взаимодействии с ботом он будет обновлен
            temp_user_id = -int(time.time()) - random.randint(1, 1000)  # Используем текущее время и случайное число как временный ID
            now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
            
            # Добавляем пользователя в таблицу users
            if db_type == 'postgres':
                cursor.execute(
                    "INSERT INTO users (user_id, username, registration_date, is_admin) VALUES (%s, %s, %s, %s)", 
                    (temp_user_id, username, now, False)
                )
            else:
                cursor.execute(
                    "INSERT INTO users (user_id, username, registration_date, is_admin) VALUES (?, ?, ?, ?)", 
                    (temp_user_id, username, now, 0)
                )
            
            added_count += 1
            print(f"Добавлен новый пользователь @{username} с временным ID {temp_user_id}")
            log_action(user_id, 'add_user', f'username:@{username}, direct_add:true, temp_id:{temp_user_id}')
    
    conn.commit()
    conn.close()
    
    # Формируем отчет
    report = f"*Результаты добавления пользователей:*\n\n"
    report += f"Добавлено новых пользователей: {added_count}\n"
    report += f"Уже существуют в базе: {already_exists_count}\n"
    report += f"Всего обработано пользователей: {len(clean_usernames)}"
    
    # Добавляем информацию о том, что пользователи добавлены и что нужно сделать дальше
    report += "\n\nВсе пользователи успешно добавлены в базу данных."
    report += "\nДля проверки используйте команду /checkusers с теми же пользователями."
    
    update.message.reply_text(report, parse_mode=ParseMode.MARKDOWN)

def remove_user(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text('У вас нет прав для выполнения этой команды.')
        return
    
    if not context.args:
        update.message.reply_text('Пожалуйста, укажите Telegram ID или @username пользователя.')
        return
    
    user_identifier = context.args[0]
    
    conn, db_type = get_db_connection()
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
        
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM users WHERE user_id = %s", (remove_user_id,))
        else:
            cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (remove_user_id,))
            
        if not cursor.fetchone():
            update.message.reply_text(f'Пользователь с ID {remove_user_id} не найден.')
            conn.close()
            return
        
        if db_type == 'postgres':
            cursor.execute("DELETE FROM users WHERE user_id = %s", (remove_user_id,))
        else:
            cursor.execute("DELETE FROM users WHERE user_id = ?", (remove_user_id,))
            
        conn.commit()
        conn.close()
        
        update.message.reply_text(f'Пользователь с ID {remove_user_id} успешно удален.')
        log_action(user_id, 'remove_user', f'user_id:{remove_user_id}')
    
    elif user_identifier.startswith('@'):
        # Если это @username
        username = user_identifier[1:]  # Убираем символ @
        
        # Находим пользователя по имени
        if db_type == 'postgres':
            cursor.execute("SELECT user_id, is_admin FROM users WHERE username = %s", (username,))
        else:
            cursor.execute("SELECT user_id, is_admin FROM users WHERE username = ?", (username,))
            
        user_data = cursor.fetchone()
        
        if not user_data:
            update.message.reply_text(f'Пользователь @{username} не найден.')
            conn.close()
            return
            
        remove_user_id, is_admin_flag = user_data
        
        # Проверяем, является ли пользователь администратором
        if db_type == 'postgres':
            is_admin_value = is_admin_flag is True
        else:
            is_admin_value = is_admin_flag == 1
            
        if is_admin_value:
            update.message.reply_text('Невозможно удалить администратора.')
            conn.close()
            return
        
        if db_type == 'postgres':
            cursor.execute("DELETE FROM users WHERE user_id = %s", (remove_user_id,))
        else:
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
        # Согласно предпочтениям пользователя, URL должен идти сразу после двоеточия
        # Используем явные символы новой строки вместо многострочной строки
        message_text = "Запись занятия: " + button_url + "\n\nЗапись доступна в течение 7 дней."
        
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
    actions_text = f"*Последние действия:*\n\n"
    
    for action in recent_actions:
        username, user_id, action_type, action_data, timestamp = action
        user_display = f"ID: `{user_id}`" if username is None else f"@{username}"
        action_info = f"{action_type}"
        if action_data:
            # Экранируем специальные символы Markdown
            safe_action_data = action_data.replace('*', '\\*').replace('_', '\\_').replace('`', '\\`')
            action_info += f" ({safe_action_data})"
        actions_text += f"- {user_display}: {action_info} ({timestamp})\n"
    
    update.message.reply_text(actions_text, parse_mode=ParseMode.MARKDOWN)
    log_action(user_id, 'show_actions', 'admin_command')

def init_db_command(update: Update, context: CallbackContext) -> None:
    """Инициализация базы данных через команду бота"""
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text('У вас нет прав для выполнения этой команды.')
        return
    
    try:
        # Вызываем функцию инициализации базы данных
        from init_db import init_database
        init_database()
        update.message.reply_text("✅ База данных успешно инициализирована! Все необходимые таблицы созданы.")
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка при инициализации базы данных: {str(e)}")

def diagnose_db(update: Update, context: CallbackContext) -> None:
    """Диагностика базы данных для проверки структуры и наличия пользователей"""
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text('У вас нет прав для выполнения этой команды.')
        return
    
    # Подключаемся к базе данных
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    # Получаем информацию о таблицах
    if db_type == 'postgres':
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    else:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    
    tables = cursor.fetchall()
    table_names = [table[0] for table in tables]
    
    # Получаем информацию о структуре таблицы users
    if 'users' in table_names:
        if db_type == 'postgres':
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'users'")
        else:
            cursor.execute("PRAGMA table_info(users)")
            
        columns_info = cursor.fetchall()
        
        if db_type == 'postgres':
            columns = [col[0] for col in columns_info]
        else:
            columns = [col[1] for col in columns_info]  # SQLite returns (id, name, type, notnull, default, pk)
        
        # Проверяем наличие колонки username
        has_username_column = 'username' in columns
        
        # Получаем количество пользователей
        cursor.execute("SELECT COUNT(*) FROM users")
        users_count = cursor.fetchone()[0]
        
        # Получаем первых 5 пользователей
        cursor.execute("SELECT user_id, username FROM users LIMIT 5")
        sample_users = cursor.fetchall()
    else:
        has_username_column = False
        users_count = 0
        sample_users = []
    
    # Получаем информацию о структуре таблицы pending_users
    if 'pending_users' in table_names:
        if db_type == 'postgres':
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'pending_users'")
        else:
            cursor.execute("PRAGMA table_info(pending_users)")
            
        columns_info = cursor.fetchall()
        
        if db_type == 'postgres':
            pending_columns = [col[0] for col in columns_info]
        else:
            pending_columns = [col[1] for col in columns_info]  # SQLite returns (id, name, type, notnull, default, pk)
        
        # Проверяем наличие колонки username
        has_pending_username_column = 'username' in pending_columns
        
        # Получаем количество пользователей
        cursor.execute("SELECT COUNT(*) FROM pending_users")
        pending_users_count = cursor.fetchone()[0]
    else:
        has_pending_username_column = False
        pending_users_count = 0
    
    conn.close()
    
    # Формируем отчет
    report = f"*Диагностика базы данных:*\n\n"
    report += f"Тип базы данных: {db_type}\n"
    report += f"Таблицы в базе: {', '.join(table_names)}\n\n"
    
    if 'users' in table_names:
        report += f"*Таблица users:*\n"
        report += f"Колонки: {', '.join(columns)}\n"
        report += f"Колонка username: {'Есть' if has_username_column else 'Отсутствует'}\n"
        report += f"Количество пользователей: {users_count}\n"
        
        if sample_users:
            report += "\nПримеры пользователей:\n"
            for user in sample_users:
                user_id, username = user
                report += f"- ID: {user_id}, Username: {username or 'Нет'}\n"
    else:
        report += "*Таблица users не найдена*\n"
    
    if 'pending_users' in table_names:
        report += f"\n*Таблица pending_users:*\n"
        report += f"Колонки: {', '.join(pending_columns)}\n"
        report += f"Колонка username: {'Есть' if has_pending_username_column else 'Отсутствует'}\n"
        report += f"Количество пользователей: {pending_users_count}\n"
    else:
        report += "\n*Таблица pending_users не найдена*\n"
    
    update.message.reply_text(report, parse_mode=ParseMode.MARKDOWN)
    log_action(user_id, 'diagnose_db', 'admin_command')

def check_users(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text('У вас нет прав для выполнения этой команды.')
        return
    
    # Получаем список пользователей для проверки
    if not context.args:
        update.message.reply_text('Пожалуйста, укажите список никнеймов для проверки.')
        return
    
    # Получаем текст после команды
    usernames_text = ' '.join(context.args)
    
    # Разбиваем текст на строки и пробелы, удаляем пустые строки
    usernames = [username.strip() for username in re.split(r'[\s\n]+', usernames_text) if username.strip()]
    
    # Проверяем формат никнеймов и удаляем @ если есть
    clean_usernames = []
    for username in usernames:
        if username.startswith('@'):
            clean_usernames.append(username[1:].lower())  # Удаляем символ @ и приводим к нижнему регистру
        else:
            clean_usernames.append(username.lower())  # Приводим к нижнему регистру
    
    if not clean_usernames:
        update.message.reply_text('Не удалось найти допустимые никнеймы в вашем списке.')
        return
    
    # Подключаемся к базе данных
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    # Статистика проверки
    found_in_users = 0
    found_in_pending = 0
    not_found = 0
    not_found_usernames = []
    
    # Проходим по всем никнеймам
    for username in clean_usernames:
        # Проверяем, есть ли пользователь в таблице users
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(%s) OR LOWER(username) = LOWER('@' || %s)", (username, username))
        else:
            cursor.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(?) OR LOWER(username) = LOWER('@' || ?)", (username, username))
            
        user_result = cursor.fetchone()
        
        if user_result:
            found_in_users += 1
            continue
        
        # Проверяем, есть ли пользователь в таблице pending_users
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(%s) OR LOWER(username) = LOWER('@' || %s)", (username, username))
        else:
            cursor.execute("SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(?) OR LOWER(username) = LOWER('@' || ?)", (username, username))
            
        pending_result = cursor.fetchone()
        
        if pending_result:
            found_in_pending += 1
        else:
            not_found += 1
            not_found_usernames.append(username)
    
    conn.close()
    
    # Формируем отчет
    report = f"*Результаты проверки {len(clean_usernames)} пользователей:*\n\n"
    report += f"Найдено в таблице users: {found_in_users}\n"
    report += f"Найдено в таблице pending_users: {found_in_pending}\n"
    report += f"Не найдено в базе данных: {not_found}\n"
    
    if not_found_usernames:
        # Ограничиваем список ненайденных пользователей для избежания слишком длинного сообщения
        shown_not_found = not_found_usernames[:10]
        report += "\nНе найдены:\n"
        for username in shown_not_found:
            report += f"- @{username}\n"
        
        if len(not_found_usernames) > 10:
            report += f"... и еще {len(not_found_usernames) - 10} пользователей"
    
    update.message.reply_text(report, parse_mode=ParseMode.MARKDOWN)
    log_action(user_id, 'check_users', 'admin_command')

def list_users(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text('У вас нет прав для выполнения этой команды.')
        return
    
    # Получаем опциональный параметр команды - количество пользователей для отображения
    limit = 10  # По умолчанию показываем только 10 пользователей
    if context.args and context.args[0].isdigit():
        limit = int(context.args[0])
    
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    # Получаем общее количество пользователей
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]
    
    # Получаем список пользователей с ограничением
    if db_type == 'postgres':
        cursor.execute("SELECT user_id, username, first_name, last_name, is_admin FROM users ORDER BY username LIMIT %s", (limit,))
    else:
        cursor.execute("SELECT user_id, username, first_name, last_name, is_admin FROM users ORDER BY username LIMIT ?", (limit,))
    
    users = cursor.fetchall()
    conn.close()
    
    # Формируем текст сообщения
    message = f"*Список пользователей*\n\nВсего пользователей: {total_users}\n\n"
    
    if users:
        for i, user in enumerate(users, 1):
            user_id, username, first_name, last_name, is_admin = user
            
            # Формируем отображаемое имя пользователя
            user_display = f"@{username}" if username else f"ID: {user_id}"
            
            if first_name or last_name:
                name = f"{first_name or ''} {last_name or ''}" .strip()
                if username:
                    user_display += f" ({name})"
                else:
                    user_display = f"{name} ({user_display})"
            
            # Добавляем метку администратора
            if is_admin:
                user_display += " (Админ)"
            
            message += f"{i}. {user_display}\n"
        
        # Если есть еще пользователи, которые не поместились в ограничение
        if total_users > limit:
            message += f"\nИ еще {total_users - limit} пользователей..."
            message += f"\n\nДля просмотра большего количества пользователей используйте команду /users <количество>"
    else:
        message += "Пользователи не найдены."
    
    update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
    log_action(user_id, 'list_users', 'admin_command')

def get_previous_video(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    user_id = user.id
    username = user.username
    
    if not is_user_authorized(user_id, username):
        update.message.reply_text(MSG_NOT_AUTHORIZED)
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT title, url, upload_date FROM videos ORDER BY upload_date DESC LIMIT 2")
    videos = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if len(videos) >= 2:
        title, url, date = videos[1]  # Second video is the previous one
        update.message.reply_text(
            f'*{title}*\n\n'
            f'Дата загрузки: {date}\n\n'
            f'Ссылка: {url}',
            parse_mode=ParseMode.MARKDOWN
        )
        log_action(user_id, 'get_previous_video', 'Запись занятия 22 мая')
    else:
        update.message.reply_text('Предыдущее занятие пока не доступно. Пожалуйста, попробуйте позже.')

def show_stats(update: Update, context: CallbackContext) -> None:
    """Показать статистику использования бота"""
    # Проверяем, является ли пользователь администратором
    user_id = update.effective_user.id
    if not is_admin(user_id):
        update.message.reply_text("Эта команда доступна только администраторам.")
        return

    try:
        # Подключаемся к базе данных
        result = get_db_connection()
        
        # Функция get_db_connection возвращает кортеж (conn, db_type)
        if isinstance(result, tuple) and len(result) == 2:
            conn, db_type = result
        else:
            # Если функция вернула неожиданный результат
            update.message.reply_text("Ошибка при подключении к базе данных: неверный формат соединения")
            return
        
        cursor = conn.cursor()

        # Получаем общее количество пользователей
        cursor.execute("SELECT COUNT(*) FROM users")
        total_users = cursor.fetchone()[0]

        # Получаем количество активированных пользователей
        # Проверяем наличие столбца is_active в зависимости от типа базы данных
        try:
            if db_type == 'postgres':
                # Для PostgreSQL используем новое соединение для каждого запроса, чтобы избежать проблем с транзакциями
                # Закрываем текущее соединение
                cursor.close()
                conn.close()
                
                # Создаем новое соединение
                result = get_db_connection()
                if isinstance(result, tuple) and len(result) == 2:
                    conn, db_type = result
                else:
                    update.message.reply_text("Ошибка при подключении к базе данных")
                    return
                
                cursor = conn.cursor()
                
                # Проверяем наличие столбца is_active
                try:
                    cursor.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = 'users' AND column_name = 'is_active'
                    """)
                    has_is_active = cursor.fetchone() is not None
                    
                    if has_is_active:
                        cursor.execute("SELECT COUNT(*) FROM users WHERE is_active = 1")
                    else:
                        # Если столбца нет, считаем всех пользователей активными
                        cursor.execute("SELECT COUNT(*) FROM users")
                    
                    active_users = cursor.fetchone()[0]
                except Exception as e:
                    print(f"Error checking is_active column: {e}")
                    # Если возникла ошибка, считаем всех пользователей активными
                    active_users = total_users
            else:
                # Для SQLite используем оригинальный запрос
                cursor.execute("SELECT COUNT(*) FROM users WHERE is_active = 1")
                active_users = cursor.fetchone()[0]
        except Exception as e:
            # Если возникла ошибка, считаем всех пользователей активными
            print(f"Error getting active users: {e}")
            active_users = total_users

        # Получаем количество администраторов
        try:
            if db_type == 'postgres':
                # Для PostgreSQL используем новое соединение для каждого запроса
                # Закрываем текущее соединение
                cursor.close()
                conn.close()
                
                # Создаем новое соединение
                result = get_db_connection()
                if isinstance(result, tuple) and len(result) == 2:
                    conn, db_type = result
                else:
                    update.message.reply_text("Ошибка при подключении к базе данных")
                    return
                
                cursor = conn.cursor()
                
                # Проверяем наличие столбца is_admin
                try:
                    cursor.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = 'users' AND column_name = 'is_admin'
                    """)
                    has_is_admin = cursor.fetchone() is not None
                    
                    if has_is_admin:
                        cursor.execute("SELECT COUNT(*) FROM users WHERE is_admin = 1")
                        admin_count = cursor.fetchone()[0]
                    else:
                        # Если столбца нет, считаем что администраторов нет
                        admin_count = 0
                except Exception as e:
                    print(f"Error checking is_admin column: {e}")
                    # Если возникла ошибка, считаем что администраторов нет
                    admin_count = 0
            else:
                # Для SQLite используем оригинальный запрос
                cursor.execute("SELECT COUNT(*) FROM users WHERE is_admin = 1")
                admin_count = cursor.fetchone()[0]
        except Exception as e:
            # Если возникла ошибка, считаем что администраторов нет
            print(f"Error getting admin count: {e}")
            admin_count = 0

        # Получаем количество неактивированных пользователей
        inactive_users = total_users - active_users

        # Формируем текст статистики
        stats_text = f"Статистика бота:\n"
        stats_text += f"Всего пользователей: {total_users}\n"
        stats_text += f"Запустили бота: {active_users}\n"
        stats_text += f"Администраторов: {admin_count}\n"
        stats_text += f"Добавлено, но не запустили бота: {inactive_users}\n\n"

        # Получаем список администраторов
        admins = []
        try:
            if db_type == 'postgres':
                # Для PostgreSQL используем новое соединение для каждого запроса
                # Закрываем текущее соединение
                cursor.close()
                conn.close()
                
                # Создаем новое соединение
                result = get_db_connection()
                if isinstance(result, tuple) and len(result) == 2:
                    conn, db_type = result
                else:
                    update.message.reply_text("Ошибка при подключении к базе данных")
                    return
                
                cursor = conn.cursor()
                
                # Проверяем наличие столбца is_admin
                try:
                    cursor.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = 'users' AND column_name = 'is_admin'
                    """)
                    has_is_admin = cursor.fetchone() is not None
                    
                    if has_is_admin:
                        cursor.execute("SELECT username, first_name, last_name FROM users WHERE is_admin = 1")
                        admins = cursor.fetchall()
                    else:
                        # Если столбца нет, добавляем стандартных администраторов
                        # Добавляем администратора по умолчанию
                        admins = [("admin", "Admin", ""), ("ilya_tomashevich", "Ilya", "Tomashevich")]
                except Exception as e:
                    print(f"Error getting admin list: {e}")
                    # Если возникла ошибка, добавляем стандартных администраторов
                    admins = [("admin", "Admin", ""), ("ilya_tomashevich", "Ilya", "Tomashevich")]
            else:
                # Для SQLite используем оригинальный запрос
                cursor.execute("SELECT username, first_name, last_name FROM users WHERE is_admin = 1")
                admins = cursor.fetchall()
        except Exception as e:
            # Если возникла ошибка, добавляем стандартных администраторов
            print(f"Error getting admin list: {e}")
            # Добавляем администратора по умолчанию
            admins = [("admin", "Admin", ""), ("ilya_tomashevich", "Ilya", "Tomashevich")]

        # Добавляем список администраторов
        stats_text += "Список администраторов:\n"
        for admin in admins:
            username, first_name, last_name = admin
            if username:
                admin_display = "@" + username
            else:
                admin_display = first_name + (" " + last_name if last_name else "")
            stats_text += f"- {admin_display}\n"

        stats_text += "\n"

        # --- Refresh PostgreSQL connection for video stats ---
        if db_type == 'postgres':
            logger.info("Refreshing connection for video stats in show_stats (PostgreSQL)")
            if 'cursor' in locals() and cursor:
                try:
                    cursor.close()
                except Exception as e_cursor:
                    logger.warning(f"Error closing cursor before video stats: {e_cursor}")
            if 'conn' in locals() and conn:
                try:
                    conn.close()
                except Exception as e_conn:
                    logger.warning(f"Error closing connection before video stats: {e_conn}")
            
            # Create a new connection for video stats
            result = get_db_connection()
            if isinstance(result, tuple) and len(result) == 2:
                conn, db_type = result # Update conn and db_type (although db_type should remain postgres)
            else:
                update.message.reply_text("Ошибка при подключении к базе данных для получения видеостатистики.")
                if 'conn' in locals() and conn: conn.close() # Close if something went wrong
                return
            cursor = conn.cursor()

        # Get unique dates from logs for get_video (in correct order)
        # Use coalesce to handle NULL values that may arise if there are no matches with LIKE
        # Also add a check that action_data is not NULL to avoid errors with SUBSTRING
        ordered_dates_query_postgres = """
            SELECT DISTINCT video_date_str
            FROM (
                SELECT CASE 
                        WHEN action_data LIKE 'get_video_%%' THEN SUBSTRING(action_data FROM 'get_video_(.*)')
                        ELSE NULL 
                       END AS video_date_str
                FROM user_actions 
                WHERE action_data LIKE 'get_video_%%' AND action_data IS NOT NULL
            ) AS subquery
            WHERE video_date_str IS NOT NULL
            ORDER BY CASE 
                     WHEN video_date_str LIKE '%% января' THEN 1
                     WHEN video_date_str LIKE '%% февраля' THEN 2
                     WHEN video_date_str LIKE '%% марта' THEN 3
                     WHEN video_date_str LIKE '%% апреля' THEN 4
                     WHEN video_date_str LIKE '%% мая' THEN 5
                     WHEN video_date_str LIKE '%% июня' THEN 6
                     WHEN video_date_str LIKE '%% июля' THEN 7
                     WHEN video_date_str LIKE '%% августа' THEN 8
                     WHEN video_date_str LIKE '%% сентября' THEN 9
                     WHEN video_date_str LIKE '%% октября' THEN 10
                     WHEN video_date_str LIKE '%% ноября' THEN 11
                     WHEN video_date_str LIKE '%% декабря' THEN 12
                     ELSE 13 END,
                     CAST(COALESCE(SUBSTRING(video_date_str FROM '^[0-9]+'), '0') AS INTEGER)
        """
        
        ordered_dates_query_sqlite = """
            SELECT DISTINCT SUBSTR(action_data, LENGTH('get_video_') + 1)
            FROM user_actions 
            WHERE action_data LIKE 'get_video_%%' AND action_data IS NOT NULL
            ORDER BY 
                CASE 
                    WHEN SUBSTR(action_data, LENGTH('get_video_') + 1) LIKE '%% января' THEN 1
                    WHEN SUBSTR(action_data, LENGTH('get_video_') + 1) LIKE '%% февраля' THEN 2
                    WHEN SUBSTR(action_data, LENGTH('get_video_') + 1) LIKE '%% марта' THEN 3
                    WHEN SUBSTR(action_data, LENGTH('get_video_') + 1) LIKE '%% апреля' THEN 4
                    WHEN SUBSTR(action_data, LENGTH('get_video_') + 1) LIKE '%% мая' THEN 5
                    WHEN SUBSTR(action_data, LENGTH('get_video_') + 1) LIKE '%% июня' THEN 6
                    WHEN SUBSTR(action_data, LENGTH('get_video_') + 1) LIKE '%% июля' THEN 7
                    WHEN SUBSTR(action_data, LENGTH('get_video_') + 1) LIKE '%% августа' THEN 8
                    WHEN SUBSTR(action_data, LENGTH('get_video_') + 1) LIKE '%% сентября' THEN 9
                    WHEN SUBSTR(action_data, LENGTH('get_video_') + 1) LIKE '%% октября' THEN 10
                    WHEN SUBSTR(action_data, LENGTH('get_video_') + 1) LIKE '%% ноября' THEN 11
                    WHEN SUBSTR(action_data, LENGTH('get_video_') + 1) LIKE '%% декабря' THEN 12
                    ELSE 13 END,
                CAST(SUBSTR(SUBSTR(action_data, LENGTH('get_video_') + 1), 1, INSTR(SUBSTR(action_data, LENGTH('get_video_') + 1), ' ') - 1) AS INTEGER)
        """

        try:
            if db_type == 'postgres':
                cursor.execute(ordered_dates_query_postgres)
            else:
                cursor.execute(ordered_dates_query_sqlite)
            video_date_rows = cursor.fetchall()
            ordered_dates = [row[0] for row in video_date_rows if row[0]]
        except Exception as e:
            logger.error(f"Error fetching ordered_dates for video stats: {e}")
            ordered_dates = [] # If error, video stats will be empty
            stats_text += "Ошибка при загрузке дат для статистики видео.\n"


        if ordered_dates:
            stats_text += "<b>Статистика по видео (кто получил запись):</b>\n"
            # Gather user actions by dates
            user_date_actions = defaultdict(lambda: defaultdict(list))
            try:
                if db_type == 'postgres':
                    cursor.execute("SELECT user_id, SUBSTRING(action_data FROM 'get_video_(.*)') as video_date FROM user_actions WHERE action_data LIKE 'get_video_%%' AND action_data IS NOT NULL")
                else:
                    cursor.execute("SELECT user_id, SUBSTR(action_data, LENGTH('get_video_') + 1) as video_date FROM user_actions WHERE action_data LIKE 'get_video_%%' AND action_data IS NOT NULL")
                
                user_actions_rows = cursor.fetchall()
                for u_id, video_date_str in user_actions_rows:
                    if video_date_str: # Make sure video_date_str is not None
                        user_date_actions[u_id][video_date_str].append('get_video')
            except Exception as e:
                logger.error(f"Error fetching user_actions for video_stats: {e}")
                stats_text += "Ошибка при загрузке действий пользователей для статистики видео.\n"
{{ ... }}
