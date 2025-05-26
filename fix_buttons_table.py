#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
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

def fix_buttons_table():
    """
    Исправляет таблицу buttons, добавляя колонку last_updated, если она отсутствует
    """
    print("\n===== ИСПРАВЛЕНИЕ ТАБЛИЦЫ BUTTONS =====\n")
    
    # Подключаемся к базе данных
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Проверяем существование таблицы buttons
        if db_type == 'postgres':
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'buttons'
            );
            """)
        else:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='buttons'")
            
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print("Таблица 'buttons' не существует. Создаем...")
            
            # Создаем таблицу buttons с колонкой last_updated
            if db_type == 'postgres':
                cursor.execute("""
                CREATE TABLE buttons (
                    button_number INTEGER PRIMARY KEY,
                    button_text TEXT,
                    message_text TEXT,
                    last_updated TIMESTAMP DEFAULT NOW()
                )
                """)
            else:
                cursor.execute("""
                CREATE TABLE buttons (
                    button_number INTEGER PRIMARY KEY,
                    button_text TEXT,
                    message_text TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
            print("Таблица 'buttons' успешно создана с колонкой last_updated!")
        else:
            # Проверяем наличие колонки last_updated
            if db_type == 'postgres':
                cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'buttons' 
                    AND column_name = 'last_updated'
                );
                """)
            else:
                cursor.execute("PRAGMA table_info(buttons)")
                columns = cursor.fetchall()
                column_exists = any(col[1] == 'last_updated' for col in columns)
                
            if db_type == 'postgres':
                column_exists = cursor.fetchone()[0]
            
            if not column_exists:
                print("Колонка 'last_updated' отсутствует. Добавляем...")
                
                # Добавляем колонку last_updated
                if db_type == 'postgres':
                    cursor.execute("ALTER TABLE buttons ADD COLUMN last_updated TIMESTAMP DEFAULT NOW()")
                else:
                    cursor.execute("ALTER TABLE buttons ADD COLUMN last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                    
                print("Колонка 'last_updated' успешно добавлена!")
            else:
                print("Колонка 'last_updated' уже существует.")
        
        # Выводим текущую структуру таблицы buttons
        if db_type == 'postgres':
            cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'buttons'
            """)
            
            columns = cursor.fetchall()
            print("\nТекущая структура таблицы buttons:")
            for col in columns:
                print(f"  - {col[0]}: {col[1]}")
        else:
            cursor.execute("PRAGMA table_info(buttons)")
            columns = cursor.fetchall()
            print("\nТекущая структура таблицы buttons:")
            for col in columns:
                print(f"  - {col[1]}: {col[2]}")
        
        # Выводим содержимое таблицы buttons
        cursor.execute("SELECT * FROM buttons")
        buttons = cursor.fetchall()
        
        print("\nСодержимое таблицы buttons:")
        if buttons:
            for button in buttons:
                print(f"  - Button {button[0]}: {button[1]} -> {button[2]}")
        else:
            print("  Таблица пуста.")
        
        # Сохраняем изменения
        conn.commit()
        print("\nИзменения успешно сохранены.")
        
    except Exception as e:
        print(f"\nОшибка при исправлении таблицы buttons: {e}")
        conn.rollback()
    
    conn.close()
    print("\n===== ИСПРАВЛЕНИЕ ТАБЛИЦЫ BUTTONS ЗАВЕРШЕНО =====\n")

def main():
    # Получаем строку подключения к базе данных
    if len(os.sys.argv) > 1:
        os.environ['DATABASE_URL'] = os.sys.argv[1]
    
    # Исправляем таблицу buttons
    fix_buttons_table()

if __name__ == "__main__":
    main()
