# -*- coding: utf-8 -*-

import os
import sqlite3
import psycopg2
from datetime import datetime
import pytz

# Импортируем функцию для подключения к базе данных
from db_utils import get_db_connection

def init_database():
    print("Инициализация базы данных...")
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    print(f"Используется база данных типа: {db_type}")
    
    # Создаем таблицу users, если она не существует
    if db_type == 'postgres':
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username VARCHAR(255),
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            phone_number VARCHAR(20),
            registration_date TIMESTAMP,
            is_admin BOOLEAN DEFAULT FALSE
        )
        """)
    else:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            phone_number TEXT,
            registration_date TEXT,
            is_admin INTEGER DEFAULT 0
        )
        """)
    
    # Создаем таблицу pending_users, если она не существует
    if db_type == 'postgres':
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pending_users (
            user_id BIGINT PRIMARY KEY,
            username VARCHAR(255),
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            phone_number VARCHAR(20),
            request_date TIMESTAMP
        )
        """)
    else:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pending_users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            phone_number TEXT,
            request_date TEXT
        )
        """)
    
    # Создаем таблицу logs, если она не существует
    if db_type == 'postgres':
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            username VARCHAR(255),
            action VARCHAR(255),
            action_data TEXT,
            timestamp TIMESTAMP
        )
        """)
    else:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            action TEXT,
            action_data TEXT,
            timestamp TEXT
        )
        """)
    
    # Создаем таблицу buttons, если она не существует
    if db_type == 'postgres':
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS buttons (
            id SERIAL PRIMARY KEY,
            button_key VARCHAR(255) UNIQUE,
            button_text VARCHAR(255),
            button_url TEXT
        )
        """)
    else:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS buttons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            button_key TEXT UNIQUE,
            button_text TEXT,
            button_url TEXT
        )
        """)
    
    # Проверяем, есть ли записи в таблице buttons
    cursor.execute("SELECT COUNT(*) FROM buttons")
    button_count = cursor.fetchone()[0]
    
    # Если таблица buttons пуста, добавляем начальные значения
    if button_count == 0:
        print("Добавление начальных кнопок...")
        now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
        
        # Добавляем кнопки для последнего и предыдущего видео
        if db_type == 'postgres':
            cursor.execute(
                "INSERT INTO buttons (button_key, button_text, button_url) VALUES (%s, %s, %s)",
                ('button1', 'Запись занятия 18 мая', 'https://drive.google.com/drive/folders/12iB-RCs89JyLqWwLV8pd1KidKT_84cYb?usp=drive_link')
            )
            cursor.execute(
                "INSERT INTO buttons (button_key, button_text, button_url) VALUES (%s, %s, %s)",
                ('button2', 'Запись занятия 22 мая', 'https://drive.google.com/drive/folders/12iB-RCs89JyLqWwLV8pd1KidKT_84cYb?usp=drive_link')
            )
        else:
            cursor.execute(
                "INSERT INTO buttons (button_key, button_text, button_url) VALUES (?, ?, ?)",
                ('button1', 'Запись занятия 18 мая', 'https://drive.google.com/drive/folders/12iB-RCs89JyLqWwLV8pd1KidKT_84cYb?usp=drive_link')
            )
            cursor.execute(
                "INSERT INTO buttons (button_key, button_text, button_url) VALUES (?, ?, ?)",
                ('button2', 'Запись занятия 22 мая', 'https://drive.google.com/drive/folders/12iB-RCs89JyLqWwLV8pd1KidKT_84cYb?usp=drive_link')
            )
    
    # Проверяем, есть ли администратор в таблице users
    cursor.execute("SELECT COUNT(*) FROM users WHERE is_admin = TRUE" if db_type == 'postgres' else "SELECT COUNT(*) FROM users WHERE is_admin = 1")
    admin_count = cursor.fetchone()[0]
    
    # Если нет администраторов, добавляем первого администратора
    if admin_count == 0:
        print("Добавление первого администратора...")
        admin_id = int(os.environ.get('ADMIN_ID', '123456789'))  # ID администратора из переменной окружения или значение по умолчанию
        now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
        
        if db_type == 'postgres':
            cursor.execute(
                "INSERT INTO users (user_id, username, registration_date, is_admin) VALUES (%s, %s, %s, %s)",
                (admin_id, 'admin', now, True)
            )
        else:
            cursor.execute(
                "INSERT INTO users (user_id, username, registration_date, is_admin) VALUES (?, ?, ?, ?)",
                (admin_id, 'admin', now, 1)
            )
    
    conn.commit()
    conn.close()
    print("База данных успешно инициализирована!")

if __name__ == "__main__":
    init_database()
