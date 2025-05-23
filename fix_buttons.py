#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sqlite3
import psycopg2
import logging
from datetime import datetime
import pytz

# Настройка логгирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Константы для типов баз данных
DB_TYPE_SQLITE = 'sqlite'
DB_TYPE_POSTGRES = 'postgres'

def get_db_connection():
    """
    Устанавливает соединение с базой данных.
    Возвращает объект соединения и тип базы данных.
    """
    # Проверяем, есть ли переменная окружения DATABASE_URL (используется в Railway)
    database_url = os.environ.get('DATABASE_URL')
    
    if database_url and database_url.startswith('postgres'):
        # Подключение к PostgreSQL
        logger.info("Connected to PostgreSQL database")
        conn = psycopg2.connect(database_url)
        return conn, DB_TYPE_POSTGRES
    else:
        # Подключение к SQLite
        logger.info("Connected to SQLite database at filmschool.db")
        conn = sqlite3.connect('filmschool.db')
        return conn, DB_TYPE_SQLITE

def fix_buttons_table():
    """
    Исправляет проблему с загрузкой кнопок
    """
    print("Начинаю исправление таблицы buttons...")
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    # Проверяем структуру таблицы buttons
    try:
        if db_type == DB_TYPE_POSTGRES:
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'buttons'")
            columns = [col[0] for col in cursor.fetchall()]
            print(f"Столбцы в таблице buttons: {', '.join(columns)}")
        else:
            cursor.execute("PRAGMA table_info(buttons)")
            columns = [col[1] for col in cursor.fetchall()]
            print(f"Столбцы в таблице buttons: {', '.join(columns)}")
        
        # Проверяем данные в таблице buttons
        cursor.execute("SELECT * FROM buttons")
        rows = cursor.fetchall()
        print(f"Найдено {len(rows)} записей в таблице buttons")
        
        # Загружаем кнопки из таблицы
        buttons = {}
        try:
            if 'button_key' in columns and 'button_text' in columns and 'button_url' in columns:
                # Таблица уже имеет нужную структуру
                print("Таблица buttons имеет нужную структуру")
                cursor.execute("SELECT button_key, button_text, button_url FROM buttons")
                for row in cursor.fetchall():
                    button_key, button_text, button_url = row
                    if button_key.startswith('button') and button_key[6:].isdigit():
                        button_number = int(button_key[6:])
                        buttons[button_number] = {'text': button_text, 'message': button_url}
                        print(f"Загружена кнопка {button_number}: {button_text} -> {button_url}")
            elif 'button_number' in columns and 'button_text' in columns and 'message_text' in columns:
                # Старая структура таблицы
                print("Таблица buttons имеет старую структуру, необходимо мигрировать")
                cursor.execute("SELECT button_number, button_text, message_text FROM buttons")
                for row in cursor.fetchall():
                    button_number, button_text, message_text = row
                    buttons[button_number] = {'text': button_text, 'message': message_text}
                    print(f"Загружена кнопка {button_number}: {button_text} -> {message_text}")
            else:
                print("Неизвестная структура таблицы buttons")
        except Exception as e:
            print(f"Ошибка при загрузке кнопок: {e}")
    except Exception as e:
        print(f"Ошибка при проверке структуры таблицы: {e}")
    
    conn.close()
    print("Исправление завершено")

if __name__ == "__main__":
    fix_buttons_table()
