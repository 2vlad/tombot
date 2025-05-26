#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import psycopg2
import sqlite3
import os
from datetime import datetime
import pytz
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('VideoAnalyzer')

class VideoDownloadsAnalyzer:
    """Класс для анализа скачиваний видео из базы данных"""
    
    def __init__(self, db_type='sqlite', database=None, host=None, user=None, password=None, port=None):
        """Инициализация анализатора
        
        Args:
            db_type: тип базы данных ('sqlite' или 'postgresql')
            database: имя базы данных или путь к файлу SQLite
            host: хост для PostgreSQL
            user: пользователь для PostgreSQL
            password: пароль для PostgreSQL
            port: порт для PostgreSQL
        """
        self.db_type = db_type.lower()
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.conn = None
        self.cursor = None
        
        # Определяем известные даты занятий
        self.known_dates = ['18 мая', '22 мая', '25 мая']
        
        # Маппинг действий к датам
        self.action_to_date_map = {
            'get_video_2': '18 мая',
            'get_video_18 мая': '18 мая',
            'get_previous_video': '22 мая',
            'get_video_22 мая': '22 мая',
            'get_latest_video': '25 мая',
            'get_video_25 мая': '25 мая'
        }
    
    def connect(self):
        """Подключение к базе данных"""
        try:
            if self.db_type == 'sqlite':
                self.conn = sqlite3.connect(self.database)
                logger.info(f"Подключено к SQLite базе данных: {self.database}")
            elif self.db_type == 'postgresql':
                self.conn = psycopg2.connect(
                    host=self.host,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    port=self.port
                )
                logger.info(f"Подключено к PostgreSQL базе данных на {self.host}")
            else:
                raise ValueError(f"Неподдерживаемый тип базы данных: {self.db_type}")
                
            self.cursor = self.conn.cursor()
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к базе данных: {e}")
            return False
    
    def disconnect(self):
        """Закрытие соединения с базой данных"""
        if self.conn:
            if self.cursor:
                self.cursor.close()
            self.conn.close()
            logger.info("Соединение с базой данных закрыто")
    
    def get_video_downloads(self):
        """Получение данных о скачиваниях видео по датам
        
        Returns:
            dict: словарь {дата: [список пользователей]}
        """
        if not self.conn:
            if not self.connect():
                return {}
        
        result = {date: [] for date in self.known_dates}
        
        try:
            # Для каждой даты получаем список пользователей
            for date in self.known_dates:
                # Формируем условия для WHERE clause
                conditions = []
                params = []
                
                # Условие для action_data
                action_data_pattern = f'Запись занятия {date}'
                conditions.append("l.action_data = %s" if self.db_type == 'postgresql' else "l.action_data = ?")
                params.append(action_data_pattern)
                
                # Находим действия, соответствующие этой дате
                specific_actions = [action for action, d in self.action_to_date_map.items() if d == date]
                
                if specific_actions:
                    placeholders = ', '.join(['%s' if self.db_type == 'postgresql' else '?'] * len(specific_actions))
                    conditions.append(f"l.action IN ({placeholders})")
                    params.extend(specific_actions)
                    
                where_clause = " OR ".join(f"({c})" for c in conditions)
                
                query = f"""
                    SELECT DISTINCT l.username, l.user_id, u.first_name, u.last_name
                    FROM logs l
                    LEFT JOIN users u ON l.user_id = u.user_id
                    WHERE {where_clause}
                    ORDER BY l.username
                """
                
                self.cursor.execute(query, params)
                users = self.cursor.fetchall()
                
                # Формируем список пользователей для этой даты
                for user_row in users:
                    username, user_id, first_name, last_name = user_row
                    
                    # Формируем отображаемое имя
                    if username:
                        display_name = f"@{username}"
                    elif first_name:
                        display_name = f"{first_name}{' ' + last_name if last_name else ''}"
                    else:
                        display_name = f"ID: {user_id}"
                    
                    # Добавляем информацию о пользователе
                    user_info = {
                        'user_id': user_id,
                        'username': username,
                        'first_name': first_name,
                        'last_name': last_name,
                        'display_name': display_name
                    }
                    
                    result[date].append(user_info)
            
            return result
        except Exception as e:
            logger.error(f"Ошибка при получении данных о скачиваниях: {e}")
            return {}
    
    def get_top_active_users(self, limit=10):
        """Получение списка самых активных пользователей
        
        Args:
            limit: максимальное количество пользователей в списке
            
        Returns:
            str: отформатированный текст с топом пользователей
        """
        if not self.conn:
            if not self.connect():
                return "Ошибка подключения к базе данных"
        
        try:
            # Запрос для получения самых активных пользователей
            query = """
                SELECT l.user_id, l.username, u.first_name, u.last_name, COUNT(*) as action_count
                FROM logs l
                LEFT JOIN users u ON l.user_id = u.user_id
                GROUP BY l.user_id, l.username, u.first_name, u.last_name
                ORDER BY action_count DESC
                LIMIT %s
            """ if self.db_type == 'postgresql' else """
                SELECT l.user_id, l.username, u.first_name, u.last_name, COUNT(*) as action_count
                FROM logs l
                LEFT JOIN users u ON l.user_id = u.user_id
                GROUP BY l.user_id, l.username, u.first_name, u.last_name
                ORDER BY action_count DESC
                LIMIT ?
            """
            
            self.cursor.execute(query, (limit,))
            top_users = self.cursor.fetchall()
            
            # Формируем отчет
            report = f"Топ-{len(top_users)} активных пользователей:\n\n"
            
            for i, (user_id, username, first_name, last_name, action_count) in enumerate(top_users, 1):
                # Формируем отображаемое имя
                if username:
                    display_name = f"@{username}"
                elif first_name:
                    display_name = f"{first_name}{' ' + last_name if last_name else ''}"
                else:
                    display_name = f"ID: {user_id}"
                
                report += f"{i}. {display_name} - {action_count} действий\n"
            
            return report
        except Exception as e:
            logger.error(f"Ошибка при получении топа активных пользователей: {e}")
            return f"Ошибка: {e}"
    
    def analyze_downloads(self, output_format='console'):
        """Анализ скачиваний видео и формирование отчета
        
        Args:
            output_format: формат вывода ('console', 'json', 'text')
            
        Returns:
            str: отчет в выбранном формате
        """
        # Получаем данные о скачиваниях
        downloads = self.get_video_downloads()
        
        if not downloads:
            return "Нет данных о скачиваниях" if output_format != 'json' else json.dumps({"error": "Нет данных"})
        
        # Формируем отчет в зависимости от формата
        if output_format == 'json':
            # Преобразуем данные для JSON
            json_data = {}
            for date, users in downloads.items():
                json_data[date] = {
                    'count': len(users),
                    'users': users
                }
            return json.dumps(json_data, ensure_ascii=False, indent=2)
        
        elif output_format == 'text':
            # Краткий текстовый отчет
            report = "ОТЧЕТ О СКАЧИВАНИЯХ ВИДЕО\n"
            report += "======================\n\n"
            
            for date in self.known_dates:
                users = downloads.get(date, [])
                report += f"Запись {date}: {len(users)} пользователей\n"
            
            return report
        
        else:  # console - подробный отчет для консоли
            report = "ОТЧЕТ О СКАЧИВАНИЯХ ВИДЕО\n"
            report += "======================\n\n"
            
            for date in self.known_dates:
                users = downloads.get(date, [])
                report += f"Запись занятия {date} получили: {len(users)}\n"
                
                for user in users:
                    report += f"- {user['display_name']}\n"
                
                report += "\n"
            
            return report

# Пример использования для тестирования
if __name__ == "__main__":
    # Для локального тестирования
    analyzer = VideoDownloadsAnalyzer(
        db_type='sqlite',
        database='filmschool.db'  # Путь к локальной базе данных
    )
    
    try:
        # Подключаемся к базе
        if analyzer.connect():
            # Получаем и выводим отчет
            report = analyzer.analyze_downloads('console')
            print(report)
            
            # Получаем топ активных пользователей
            top_users = analyzer.get_top_active_users(5)
            print("\n" + top_users)
    finally:
        # Закрываем соединение
        analyzer.disconnect()
