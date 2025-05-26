#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import os
import sqlite3
import psycopg2
from datetime import datetime
import pytz
import time
import random

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

def add_users_directly():
    """
    Добавляет пользователей напрямую в таблицу users
    """
    usernames = ['nastyaglukhikh', 'sebastianbach', 'valerigeb', 'jchvanova', 'mabublik', 'nikitafateev', 'oxanatimchenko']
    
    print("Начинаю добавление пользователей в базу данных...")
    
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
    print("Добавление пользователей завершено")

if __name__ == "__main__":
    add_users_directly()
