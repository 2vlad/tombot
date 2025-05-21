#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import os

def migrate_database():
    print("Начинаем миграцию базы данных...")
    
    # Подключаемся к базе данных
    conn = sqlite3.connect('filmschool.db')
    cursor = conn.cursor()
    
    # Проверяем, есть ли новые столбцы в таблице logs
    cursor.execute("PRAGMA table_info(logs)")
    columns = [column[1] for column in cursor.fetchall()]
    
    # Если новых столбцов нет, добавляем их
    if 'username' not in columns:
        print("Добавляем новые столбцы в таблицу logs...")
        try:
            # Переименовываем старую таблицу
            cursor.execute("ALTER TABLE logs RENAME TO logs_old")
            
            # Создаем новую таблицу с нужной структурой
            cursor.execute('''
            CREATE TABLE logs (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                action TEXT,
                action_data TEXT,
                timestamp TEXT,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
            ''')
            
            # Копируем данные из старой таблицы в новую
            cursor.execute('''
            INSERT INTO logs (id, user_id, action, timestamp)
            SELECT id, user_id, action, timestamp FROM logs_old
            ''')
            
            # Обновляем данные username, first_name, last_name из таблицы users
            cursor.execute('''
            UPDATE logs
            SET username = (SELECT username FROM users WHERE logs.user_id = users.user_id),
                first_name = (SELECT first_name FROM users WHERE logs.user_id = users.user_id),
                last_name = (SELECT last_name FROM users WHERE logs.user_id = users.user_id)
            ''')
            
            # Удаляем старую таблицу
            cursor.execute("DROP TABLE logs_old")
            
            print("Миграция таблицы logs успешно завершена!")
        except Exception as e:
            conn.rollback()
            print(f"Ошибка при миграции: {e}")
            return False
    else:
        print("Таблица logs уже имеет нужную структуру.")
    
    # Сохраняем изменения и закрываем соединение
    conn.commit()
    conn.close()
    
    print("Миграция базы данных успешно завершена!")
    return True

if __name__ == '__main__':
    migrate_database()
