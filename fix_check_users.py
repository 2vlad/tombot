#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import os
import sqlite3
import psycopg2
from datetime import datetime
import pytz

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

def fix_check_users():
    """
    Проверяет наличие пользователей в базе данных и выводит статистику.
    """
    print("Начинаю проверку пользователей в базе данных...")
    
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
    
    print(f"Найдены таблицы: {', '.join(table_names)}")
    
    # Проверяем наличие таблицы users
    if 'users' in table_names:
        # Получаем информацию о структуре таблицы users
        if db_type == 'postgres':
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'users'")
        else:
            cursor.execute("PRAGMA table_info(users)")
            
        columns_info = cursor.fetchall()
        
        if db_type == 'postgres':
            columns = [col[0] for col in columns_info]
        else:
            columns = [col[1] for col in columns_info]  # SQLite returns (id, name, type, notnull, default, pk)
        
        print(f"Столбцы в таблице users: {', '.join(columns)}")
        
        # Проверяем наличие колонки username
        has_username_column = 'username' in columns
        
        if has_username_column:
            # Получаем количество пользователей
            cursor.execute("SELECT COUNT(*) FROM users")
            users_count = cursor.fetchone()[0]
            
            print(f"Количество пользователей в таблице users: {users_count}")
            
            # Получаем первых 5 пользователей
            cursor.execute("SELECT user_id, username FROM users LIMIT 5")
            sample_users = cursor.fetchall()
            
            print("Примеры пользователей:")
            for user in sample_users:
                user_id, username = user
                print(f"ID: {user_id}, Username: {username or 'Нет'}")
        else:
            print("В таблице users отсутствует колонка username")
    else:
        print("Таблица users не найдена")
    
    # Проверяем наличие таблицы pending_users
    if 'pending_users' in table_names:
        # Получаем информацию о структуре таблицы pending_users
        if db_type == 'postgres':
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'pending_users'")
        else:
            cursor.execute("PRAGMA table_info(pending_users)")
            
        columns_info = cursor.fetchall()
        
        if db_type == 'postgres':
            pending_columns = [col[0] for col in columns_info]
        else:
            pending_columns = [col[1] for col in columns_info]  # SQLite returns (id, name, type, notnull, default, pk)
        
        print(f"Столбцы в таблице pending_users: {', '.join(pending_columns)}")
        
        # Проверяем наличие колонки username
        has_pending_username_column = 'username' in pending_columns
        
        if has_pending_username_column:
            # Получаем количество пользователей
            cursor.execute("SELECT COUNT(*) FROM pending_users")
            pending_users_count = cursor.fetchone()[0]
            
            print(f"Количество пользователей в таблице pending_users: {pending_users_count}")
            
            # Получаем первых 5 пользователей
            cursor.execute("SELECT user_id, username FROM pending_users LIMIT 5")
            sample_pending_users = cursor.fetchall()
            
            print("Примеры ожидающих пользователей:")
            for user in sample_pending_users:
                user_id, username = user
                print(f"ID: {user_id}, Username: {username or 'Нет'}")
        else:
            print("В таблице pending_users отсутствует колонка username")
    else:
        print("Таблица pending_users не найдена")
    
    # Исправляем запросы в функции check_users
    print("\nИсправляю запросы в функции check_users...")
    
    # Проверяем, есть ли пользователи в таблице users
    usernames_to_check = ['nastyaglukhikh', 'sebastianbach', 'valerigeb', 'jchvanova', 'mabublik', 'nikitafateev', 'oxanatimchenko']
    
    # Статистика проверки
    found_in_users = 0
    found_in_pending = 0
    not_found = 0
    not_found_usernames = []
    
    # Проходим по всем никнеймам
    for username in usernames_to_check:
        # Приводим имя пользователя к нижнему регистру
        username = username.lower()
        
        # Проверяем, есть ли пользователь в таблице users
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(%s)", (username,))
        else:
            cursor.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(?)", (username,))
            
        user_result = cursor.fetchone()
        
        if user_result:
            found_in_users += 1
            print(f"Пользователь @{username} найден в таблице users с ID {user_result[0]}")
            continue
        
        # Проверяем, есть ли пользователь в таблице pending_users
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(%s)", (username,))
        else:
            cursor.execute("SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(?)", (username,))
            
        pending_result = cursor.fetchone()
        
        if pending_result:
            found_in_pending += 1
            print(f"Пользователь @{username} найден в таблице pending_users с ID {pending_result[0]}")
        else:
            not_found += 1
            not_found_usernames.append(username)
            print(f"Пользователь @{username} не найден в базе данных")
    
    print("\nРезультаты проверки:")
    print(f"Найдено в таблице users: {found_in_users}")
    print(f"Найдено в таблице pending_users: {found_in_pending}")
    print(f"Не найдено в базе данных: {not_found}")
    
    if not_found_usernames:
        print("Не найдены:")
        for username in not_found_usernames:
            print(f"- @{username}")
    
    # Добавляем пользователей, которых не нашли
    if not_found_usernames and 'users' in table_names and has_username_column:
        print("\nДобавляю пользователей, которых не нашли...")
        
        for username in not_found_usernames:
            # Создаем временный ID (отрицательное число)
            import time
            import random
            temp_user_id = -int(time.time()) - random.randint(1, 1000)
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
            
            print(f"Добавлен пользователь @{username} с временным ID {temp_user_id}")
        
        conn.commit()
        print("Пользователи успешно добавлены в базу данных")
    
    conn.close()
    print("Проверка завершена")

if __name__ == "__main__":
    fix_check_users()
