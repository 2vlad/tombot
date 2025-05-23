#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import psycopg2
import sqlite3

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

def diagnose_users_table():
    """
    Диагностирует проблемы с таблицей users
    """
    print("\n===== ДИАГНОСТИКА ТАБЛИЦЫ USERS =====\n")
    
    # Подключаемся к базе данных
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Проверяем существование таблицы users
        if db_type == 'postgres':
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'users'
            );
            """)
        else:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
            
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print("Таблица 'users' не существует!")
            return
        
        # Выводим структуру таблицы users
        if db_type == 'postgres':
            cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'users'
            """)
            
            columns = cursor.fetchall()
            print("\nСтруктура таблицы users:")
            for col in columns:
                print(f"  - {col[0]}: {col[1]}")
        else:
            cursor.execute("PRAGMA table_info(users)")
            columns = cursor.fetchall()
            print("\nСтруктура таблицы users:")
            for col in columns:
                print(f"  - {col[1]}: {col[2]}")
        
        # Выводим количество пользователей в таблице
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        print(f"\nВсего пользователей в таблице users: {user_count}")
        
        # Выводим список всех пользователей
        cursor.execute("SELECT user_id, username FROM users")
        users = cursor.fetchall()
        
        print("\nСписок пользователей в таблице users:")
        for user in users:
            print(f"  - ID: {user[0]}, Username: {user[1]}")
        
        # Проверяем конкретных пользователей из скриншота
        usernames_to_check = [
            'nastyaglukhikh', 'Sebastianbachh', 'valerigeb', 'JChvanova', 
            'mabublik', 'Nikita_Fateev', 'oxanatimchenko', 'TikhanovaStory'
        ]
        
        print("\nПроверка конкретных пользователей:")
        for username in usernames_to_check:
            # Проверяем с @ и без @
            cursor.execute("SELECT user_id FROM users WHERE username = %s OR username = %s", 
                          (username, '@' + username))
            result = cursor.fetchone()
            
            if result:
                print(f"  - {username}: НАЙДЕН (ID: {result[0]})")
            else:
                print(f"  - {username}: НЕ НАЙДЕН")
        
        # Проверяем, как бот ищет пользователей
        print("\nПроверка SQL-запроса, который использует бот для поиска пользователей:")
        for username in usernames_to_check:
            # Проверяем точное совпадение (как в боте)
            cursor.execute("SELECT user_id FROM users WHERE username = %s", (username,))
            result1 = cursor.fetchone()
            
            # Проверяем с добавлением @
            cursor.execute("SELECT user_id FROM users WHERE username = %s", ('@' + username,))
            result2 = cursor.fetchone()
            
            # Проверяем с LIKE
            cursor.execute("SELECT user_id FROM users WHERE username LIKE %s", ('%' + username + '%',))
            result3 = cursor.fetchall()
            
            print(f"  - {username}:")
            print(f"    * Точное совпадение: {result1[0] if result1 else 'НЕ НАЙДЕН'}")
            print(f"    * С @: {result2[0] if result2 else 'НЕ НАЙДЕН'}")
            print(f"    * LIKE: {[r[0] for r in result3] if result3 else 'НЕ НАЙДЕН'}")
        
    except Exception as e:
        print(f"\nОшибка при диагностике таблицы users: {e}")
    
    conn.close()
    print("\n===== ДИАГНОСТИКА ТАБЛИЦЫ USERS ЗАВЕРШЕНА =====\n")

def main():
    # Получаем строку подключения к базе данных
    if len(sys.argv) > 1:
        os.environ['DATABASE_URL'] = sys.argv[1]
    
    # Диагностируем таблицу users
    diagnose_users_table()

if __name__ == "__main__":
    main()
