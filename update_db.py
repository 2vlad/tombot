#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import psycopg2
import os
import dj_database_url
from dotenv import load_dotenv

def get_db_connection():
    # Загружаем переменные окружения из .env файла
    load_dotenv()
    
    # Проверяем, есть ли переменная окружения DATABASE_URL (для Railway)
    database_url = os.environ.get('DATABASE_URL')
    
    if database_url:
        # Используем PostgreSQL на Railway
        conn = psycopg2.connect(dj_database_url.config(default=database_url))
        return conn, 'postgres'
    else:
        # Используем SQLite локально
        conn = sqlite3.connect('filmschool.db')
        return conn, 'sqlite'

def update_database():
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    print(f"Используется база данных типа: {db_type}")
    
    # Проверяем, есть ли уже колонка phone_number в таблице users
    if db_type == 'postgres':
        cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'users' AND column_name = 'phone_number'
        """)
    else:  # SQLite
        cursor.execute("PRAGMA table_info(users)")
        columns = [column[1] for column in cursor.fetchall()]
        has_phone_column = 'phone_number' in columns
    
    # Для PostgreSQL проверяем результат запроса
    if db_type == 'postgres':
        has_phone_column = cursor.fetchone() is not None
    
    # Добавляем колонку phone_number в таблицу users, если её нет
    if not has_phone_column:
        print("Добавление колонки phone_number в таблицу users...")
        if db_type == 'postgres':
            cursor.execute("ALTER TABLE users ADD COLUMN phone_number VARCHAR(20)")
        else:  # SQLite
            cursor.execute("ALTER TABLE users ADD COLUMN phone_number TEXT")
    else:
        print("Колонка phone_number уже существует в таблице users")
    
    # Проверяем, есть ли уже колонка phone_number в таблице pending_users
    if db_type == 'postgres':
        cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'pending_users' AND column_name = 'phone_number'
        """)
    else:  # SQLite
        cursor.execute("PRAGMA table_info(pending_users)")
        columns = [column[1] for column in cursor.fetchall()]
        has_phone_column = 'phone_number' in columns
    
    # Для PostgreSQL проверяем результат запроса
    if db_type == 'postgres':
        has_phone_column = cursor.fetchone() is not None
    
    # Добавляем колонку phone_number в таблицу pending_users, если её нет
    if not has_phone_column:
        print("Добавление колонки phone_number в таблицу pending_users...")
        if db_type == 'postgres':
            cursor.execute("ALTER TABLE pending_users ADD COLUMN phone_number VARCHAR(20)")
        else:  # SQLite
            cursor.execute("ALTER TABLE pending_users ADD COLUMN phone_number TEXT")
    else:
        print("Колонка phone_number уже существует в таблице pending_users")
    
    # Применяем изменения
    conn.commit()
    print("База данных успешно обновлена!")
    
    # Закрываем соединение
    conn.close()

if __name__ == "__main__":
    update_database()
