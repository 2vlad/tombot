#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import psycopg2
import sqlite3
import re

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

def fix_stats_and_checkusers():
    """
    Исправляет команды /stats и /checkusers
    """
    print("\n===== ИСПРАВЛЕНИЕ КОМАНД /stats И /checkusers =====\n")
    
    # Путь к файлу bot.py
    bot_py_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bot.py')
    
    # Проверяем, существует ли файл
    if not os.path.exists(bot_py_path):
        print(f"\nОшибка: файл {bot_py_path} не найден")
        return
    
    # Подключаемся к базе данных для тестирования
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Создаем резервную копию bot.py
        backup_path = bot_py_path + '.stats_checkusers.bak'
        with open(bot_py_path, 'r', encoding='utf-8') as f_in, open(backup_path, 'w', encoding='utf-8') as f_out:
            f_out.write(f_in.read())
        
        print(f"  - Создана резервная копия {backup_path}")
        
        # Читаем файл bot.py
        with open(bot_py_path, 'r', encoding='utf-8') as f:
            bot_py_content = f.read()
        
        # 1. Исправляем функцию show_stats
        print("\n1. Исправляем функцию show_stats...")
        
        # Исправляем подключение к базе данных в функции show_stats
        old_code_stats_1 = "conn = sqlite3.connect('filmschool.db')\n    cursor = conn.cursor()"
        new_code_stats_1 = "conn, db_type = get_db_connection()\n    cursor = conn.cursor()"
        
        # Исправляем SQL-запросы в функции show_stats
        old_code_stats_2 = "    # Total admins\n    cursor.execute(\"SELECT COUNT(*) FROM users WHERE is_admin = 1\")\n    total_admins = cursor.fetchone()[0]"
        
        new_code_stats_2 = "    # Total admins\n    if db_type == 'postgres':\n        cursor.execute(\"SELECT COUNT(*) FROM users WHERE is_admin = TRUE\")\n    else:\n        cursor.execute(\"SELECT COUNT(*) FROM users WHERE is_admin = 1\")\n    total_admins = cursor.fetchone()[0]"
        
        # Исправляем GROUP_CONCAT на PostgreSQL-совместимый вариант для latest_video
        old_code_stats_3 = """    cursor.execute("""
    SELECT username, first_name, last_name, user_id, GROUP_CONCAT(timestamp, ', ') as access_times
    FROM logs 
    WHERE action = 'get_latest_video'
    GROUP BY COALESCE(username, user_id)
    ORDER BY MAX(timestamp) DESC
    """)
    latest_video_users = cursor.fetchall()"""
        
        new_code_stats_3 = """    if db_type == 'postgres':
        cursor.execute("""
        SELECT username, first_name, last_name, user_id, STRING_AGG(timestamp::text, ', ') as access_times
        FROM logs 
        WHERE action = 'get_latest_video'
        GROUP BY COALESCE(username, user_id), username, first_name, last_name, user_id
        ORDER BY MAX(timestamp) DESC
        """)
    else:
        cursor.execute("""
        SELECT username, first_name, last_name, user_id, GROUP_CONCAT(timestamp, ', ') as access_times
        FROM logs 
        WHERE action = 'get_latest_video'
        GROUP BY COALESCE(username, user_id)
        ORDER BY MAX(timestamp) DESC
        """)
    latest_video_users = cursor.fetchall()"""
        
        # Исправляем GROUP_CONCAT на PostgreSQL-совместимый вариант для previous_video
        old_code_stats_4 = """    cursor.execute("""
    SELECT username, first_name, last_name, user_id, GROUP_CONCAT(timestamp, ', ') as access_times
    FROM logs 
    WHERE action = 'get_previous_video'
    GROUP BY COALESCE(username, user_id)
    ORDER BY MAX(timestamp) DESC
    """)
    previous_video_users = cursor.fetchall()"""
        
        new_code_stats_4 = """    if db_type == 'postgres':
        cursor.execute("""
        SELECT username, first_name, last_name, user_id, STRING_AGG(timestamp::text, ', ') as access_times
        FROM logs 
        WHERE action = 'get_previous_video'
        GROUP BY COALESCE(username, user_id), username, first_name, last_name, user_id
        ORDER BY MAX(timestamp) DESC
        """)
    else:
        cursor.execute("""
        SELECT username, first_name, last_name, user_id, GROUP_CONCAT(timestamp, ', ') as access_times
        FROM logs 
        WHERE action = 'get_previous_video'
        GROUP BY COALESCE(username, user_id)
        ORDER BY MAX(timestamp) DESC
        """)
    previous_video_users = cursor.fetchall()"""
        
        # 2. Исправляем функцию check_users
        print("2. Исправляем функцию check_users...")
        
        # Исправляем запрос к таблице pending_users
        old_code_check_1 = """        # Проверяем, есть ли пользователь в таблице pending_users
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM pending_users WHERE username = %s", (username,))
        else:
            cursor.execute("SELECT user_id FROM pending_users WHERE username = ?", (username,))"""
        
        new_code_check_1 = """        # Проверяем, есть ли пользователь в таблице pending_users
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(%s) OR LOWER(username) = LOWER('@' || %s)", (username, username))
        else:
            cursor.execute("SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(?) OR LOWER(username) = LOWER('@' || ?)", (username, username))"""
        
        # Заменяем код
        bot_py_content = bot_py_content.replace(old_code_stats_1, new_code_stats_1)
        bot_py_content = bot_py_content.replace(old_code_stats_2, new_code_stats_2)
        bot_py_content = bot_py_content.replace(old_code_stats_3, new_code_stats_3)
        bot_py_content = bot_py_content.replace(old_code_stats_4, new_code_stats_4)
        bot_py_content = bot_py_content.replace(old_code_check_1, new_code_check_1)
        
        # Записываем изменения в файл
        with open(bot_py_path, 'w', encoding='utf-8') as f:
            f.write(bot_py_content)
        
        print("  - Функции show_stats и check_users успешно исправлены")
        print("  - Теперь команды /stats и /checkusers будут работать корректно с PostgreSQL")
        
    except Exception as e:
        print(f"\nОшибка при исправлении команд /stats и /checkusers: {e}")
    
    conn.close()
    print("\n===== ИСПРАВЛЕНИЕ КОМАНД /stats И /checkusers ЗАВЕРШЕНО =====\n")

def main():
    # Получаем строку подключения к базе данных
    if len(sys.argv) > 1:
        os.environ['DATABASE_URL'] = sys.argv[1]
    
    # Исправляем команды /stats и /checkusers
    fix_stats_and_checkusers()

if __name__ == "__main__":
    main()
