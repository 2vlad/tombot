#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sqlite3
import psycopg2
from datetime import datetime
import pytz
import time
import random
import sys

# Инструкции по использованию на Railway:
# 1. Загрузите этот скрипт на Railway вместе с вашим ботом
# 2. Запустите команду: python diagnose_and_fix_db.py diagnose
#    Это покажет текущее состояние базы данных
# 3. Если таблицы не существуют, запустите: python diagnose_and_fix_db.py create_tables
# 4. Для добавления пользователей запустите: python diagnose_and_fix_db.py add_users user1 user2 user3 ...

# Подключаемся к базе данных
def get_db_connection():
    # Проверяем, есть ли переменная окружения DATABASE_URL (используется в Railway)
    database_url = os.environ.get('DATABASE_URL')
    
    if database_url and database_url.startswith('postgres'):
        # Подключение к PostgreSQL
        print("Connected to PostgreSQL database")
        conn = psycopg2.connect(database_url)
        return conn, 'postgres'
    else:
        # Подключение к SQLite
        print("Connected to SQLite database at filmschool.db")
        conn = sqlite3.connect('filmschool.db')
        return conn, 'sqlite'

def diagnose_database():
    """
    Диагностика базы данных для проверки структуры и наличия пользователей
    """
    print("\n===== ДИАГНОСТИКА БАЗЫ ДАННЫХ =====\n")
    
    # Подключаемся к базе данных
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    # Проверяем наличие таблиц
    tables = []
    if db_type == 'postgres':
        cursor.execute("""SELECT table_name FROM information_schema.tables 
                      WHERE table_schema = 'public' AND table_type = 'BASE TABLE'""")
        tables = [table[0] for table in cursor.fetchall()]
    else:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [table[0] for table in cursor.fetchall()]
    
    print(f"Найдено таблиц в базе данных: {len(tables)}")
    print(f"Список таблиц: {', '.join(tables)}")
    print()
    
    # Проверяем структуру таблицы users
    if 'users' in tables:
        print("Таблица 'users' найдена.")
        
        # Получаем структуру таблицы
        if db_type == 'postgres':
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'users'
            """)
        else:
            cursor.execute("PRAGMA table_info(users)")
        
        columns = cursor.fetchall()
        if db_type == 'postgres':
            print("Структура таблицы users:")
            for col in columns:
                print(f"  - {col[0]}: {col[1]}")
        else:
            print("Структура таблицы users:")
            for col in columns:
                print(f"  - {col[1]}: {col[2]}")
        
        # Проверяем количество пользователей
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        print(f"\nКоличество пользователей в таблице users: {user_count}")
        
        if user_count > 0:
            # Выводим несколько примеров пользователей
            if db_type == 'postgres':
                cursor.execute("SELECT user_id, username, registration_date, is_admin FROM users LIMIT 5")
            else:
                cursor.execute("SELECT user_id, username, registration_date, is_admin FROM users LIMIT 5")
            
            users = cursor.fetchall()
            print("\nПримеры пользователей:")
            for user in users:
                print(f"  - ID: {user[0]}, Username: {user[1]}, Дата регистрации: {user[2]}, Админ: {user[3]}")
    else:
        print("Таблица 'users' НЕ найдена!")
    
    # Проверяем структуру таблицы pending_users
    if 'pending_users' in tables:
        print("\nТаблица 'pending_users' найдена.")
        
        # Получаем структуру таблицы
        if db_type == 'postgres':
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'pending_users'
            """)
        else:
            cursor.execute("PRAGMA table_info(pending_users)")
        
        columns = cursor.fetchall()
        if db_type == 'postgres':
            print("Структура таблицы pending_users:")
            for col in columns:
                print(f"  - {col[0]}: {col[1]}")
        else:
            print("Структура таблицы pending_users:")
            for col in columns:
                print(f"  - {col[1]}: {col[2]}")
        
        # Проверяем количество ожидающих пользователей
        cursor.execute("SELECT COUNT(*) FROM pending_users")
        pending_count = cursor.fetchone()[0]
        print(f"\nКоличество пользователей в таблице pending_users: {pending_count}")
        
        if pending_count > 0:
            # Выводим несколько примеров ожидающих пользователей
            if db_type == 'postgres':
                cursor.execute("SELECT user_id, username, registration_date FROM pending_users LIMIT 5")
            else:
                cursor.execute("SELECT user_id, username, registration_date FROM pending_users LIMIT 5")
            
            pending_users = cursor.fetchall()
            print("\nПримеры ожидающих пользователей:")
            for user in pending_users:
                print(f"  - ID: {user[0]}, Username: {user[1]}, Дата регистрации: {user[2]}")
    else:
        print("\nТаблица 'pending_users' НЕ найдена!")
    
    conn.close()
    print("\n===== ДИАГНОСТИКА ЗАВЕРШЕНА =====\n")

def create_tables():
    """
    Создает необходимые таблицы в базе данных, если они не существуют
    """
    print("\n===== СОЗДАНИЕ ТАБЛИЦ =====\n")
    
    # Подключаемся к базе данных
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    # Создаем таблицу users, если она не существует
    if db_type == 'postgres':
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            registration_date TIMESTAMP,
            is_admin BOOLEAN DEFAULT FALSE
        )
        """)
    else:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            registration_date TEXT,
            is_admin INTEGER DEFAULT 0
        )
        """)
    
    # Создаем таблицу pending_users, если она не существует
    if db_type == 'postgres':
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pending_users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            registration_date TIMESTAMP
        )
        """)
    else:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pending_users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            registration_date TEXT
        )
        """)
    
    # Создаем таблицу buttons, если она не существует
    if db_type == 'postgres':
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS buttons (
            button_number INTEGER PRIMARY KEY,
            button_text TEXT,
            message_text TEXT
        )
        """)
    else:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS buttons (
            button_number INTEGER PRIMARY KEY,
            button_text TEXT,
            message_text TEXT
        )
        """)
    
    # Создаем таблицу videos, если она не существует
    if db_type == 'postgres':
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            video_id SERIAL PRIMARY KEY,
            video_file_id TEXT,
            upload_date TIMESTAMP,
            description TEXT
        )
        """)
    else:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            video_id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_file_id TEXT,
            upload_date TEXT,
            description TEXT
        )
        """)
    
    # Создаем таблицу actions, если она не существует
    if db_type == 'postgres':
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS actions (
            action_id SERIAL PRIMARY KEY,
            user_id BIGINT,
            action_type TEXT,
            action_data TEXT,
            action_date TIMESTAMP
        )
        """)
    else:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS actions (
            action_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            action_type TEXT,
            action_data TEXT,
            action_date TEXT
        )
        """)
    
    conn.commit()
    conn.close()
    print("Таблицы успешно созданы или уже существуют.")
    print("\n===== СОЗДАНИЕ ТАБЛИЦ ЗАВЕРШЕНО =====\n")

def add_users_directly(usernames=None):
    """
    Добавляет пользователей напрямую в таблицу users
    
    Args:
        usernames (list, optional): Список имен пользователей для добавления. 
                                   Если None, запрашивает ввод у пользователя.
    """
    if usernames is None:
        # Запрашиваем список пользователей у пользователя
        print("\n===== ДОБАВЛЕНИЕ ПОЛЬЗОВАТЕЛЕЙ =====\n")
        print("Введите имена пользователей (без @), разделенные пробелами или запятыми:")
        input_usernames = input("> ")
        
        # Разбиваем ввод на отдельные имена пользователей
        usernames = [username.strip() for username in re.split(r'[\s,]+', input_usernames) if username.strip()]
    
    if not usernames:
        print("Не указаны имена пользователей для добавления.")
        return
    
    print(f"Начинаю добавление {len(usernames)} пользователей в базу данных...")
    
    # Подключаемся к базе данных
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    added_count = 0
    already_exists_count = 0
    
    for username in usernames:
        username = username.lower()
        
        # Проверяем, есть ли пользователь уже в таблице users
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(%s)", (username,))
        else:
            cursor.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(?)", (username,))
            
        existing_user = cursor.fetchone()
        
        if existing_user:
            print(f"Пользователь @{username} уже существует с ID {existing_user[0]}")
            already_exists_count += 1
            continue
        
        # Создаем временный ID (отрицательное число)
        temp_user_id = -int(time.time()) - random.randint(1, 1000)
        now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
        
        # Добавляем пользователя в таблицу users
        try:
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
            
            print(f"Добавлен пользователь @{username} с ID {temp_user_id}")
            added_count += 1
            
        except Exception as e:
            print(f"Ошибка при добавлении пользователя @{username}: {e}")
    
    # Сохраняем изменения
    conn.commit()
    
    # Проверяем результат
    print(f"\nРезультаты:")
    print(f"Добавлено новых пользователей: {added_count}")
    print(f"Уже существовали: {already_exists_count}")
    
    # Проверяем, что пользователи действительно добавлены
    print("\nПроверяем добавленных пользователей:")
    for username in usernames:
        username = username.lower()
        if db_type == 'postgres':
            cursor.execute("SELECT user_id, username FROM users WHERE LOWER(username) = LOWER(%s)", (username,))
        else:
            cursor.execute("SELECT user_id, username FROM users WHERE LOWER(username) = LOWER(?)", (username,))
            
        user = cursor.fetchone()
        if user:
            print(f"✓ @{username} найден с ID {user[0]}")
        else:
            print(f"✗ @{username} НЕ найден")
    
    conn.close()
    print("\n===== ДОБАВЛЕНИЕ ПОЛЬЗОВАТЕЛЕЙ ЗАВЕРШЕНО =====\n")

def patch_check_users():
    """
    Исправляет функцию check_users в файле bot.py для использования LOWER() при проверке pending_users
    """
    print("\n===== ИСПРАВЛЕНИЕ ФУНКЦИИ CHECK_USERS =====\n")
    
    try:
        with open('bot.py', 'r', encoding='utf-8') as file:
            content = file.read()
        
        # Ищем проблемную строку в функции check_users
        old_code = "cursor.execute(\"SELECT user_id FROM pending_users WHERE username = %s\", (username,))"
        new_code = "cursor.execute(\"SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(%s)\", (username,))"
        
        if old_code in content:
            # Заменяем проблемную строку
            content = content.replace(old_code, new_code)
            
            # Сохраняем изменения
            with open('bot.py', 'w', encoding='utf-8') as file:
                file.write(content)
            
            print("Функция check_users успешно исправлена для использования регистронезависимого сравнения в таблице pending_users.")
        else:
            print("Проблемная строка не найдена или уже исправлена.")
    
    except Exception as e:
        print(f"Ошибка при исправлении функции check_users: {e}")
    
    print("\n===== ИСПРАВЛЕНИЕ ФУНКЦИИ CHECK_USERS ЗАВЕРШЕНО =====\n")

def show_menu():
    """
    Показывает меню с доступными действиями
    """
    print("\n===== МЕНЮ =====")
    print("1. Диагностика базы данных")
    print("2. Создать таблицы в базе данных")
    print("3. Добавить пользователей напрямую")
    print("4. Исправить функцию check_users")
    print("5. Выход")
    print("=============\n")

def main():
    print("\n===== УТИЛИТА ДИАГНОСТИКИ И ИСПРАВЛЕНИЯ БАЗЫ ДАННЫХ =====\n")
    print("Эта утилита поможет диагностировать и исправить проблемы с базой данных.")
    
    while True:
        show_menu()
        choice = input("Выберите действие (1-5): ")
        
        if choice == '1':
            diagnose_database()
        elif choice == '2':
            create_tables()
        elif choice == '3':
            add_users_directly()
        elif choice == '4':
            patch_check_users()
        elif choice == '5':
            print("\nВыход из программы.")
            break
        else:
            print("\nНеверный выбор. Пожалуйста, выберите число от 1 до 5.")

if __name__ == "__main__":
    # Если скрипт запущен с аргументами командной строки
    if len(sys.argv) > 1:
        if sys.argv[1] == "diagnose":
            diagnose_database()
        elif sys.argv[1] == "create_tables":
            create_tables()
        elif sys.argv[1] == "add_users" and len(sys.argv) > 2:
            # Получаем список пользователей из аргументов командной строки
            usernames = sys.argv[2:]
            add_users_directly(usernames)
        elif sys.argv[1] == "patch_check_users":
            patch_check_users()
        else:
            print("Использование:")
            print("  python diagnose_and_fix_db.py diagnose - Диагностика базы данных")
            print("  python diagnose_and_fix_db.py create_tables - Создание таблиц")
            print("  python diagnose_and_fix_db.py add_users user1 user2 ... - Добавление пользователей")
            print("  python diagnose_and_fix_db.py patch_check_users - Исправление функции check_users")
    else:
        # Запускаем интерактивный режим
        main()
