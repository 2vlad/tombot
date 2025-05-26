#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import psycopg2
import sqlite3
import os
from datetime import datetime
import pytz
import logging
from names_loader import NamesLoader

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
        
        # Загрузим данные о пользователях из CSV
        try:
            self.names_loader = NamesLoader()
            logger.info("Данные о пользователях успешно загружены")
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных о пользователях: {e}")
            self.names_loader = None
        
        # Соответствие действий датам
        self.action_to_date_map = {
            'get_video_18 мая': '18 мая',
            'get_latest_video': '18 мая',  # Последнее видео - это 18 мая
            'get_video_22 мая': '22 мая',
            'get_previous_video': '22 мая',  # Предыдущее видео - это 22 мая
            'get_video_25 мая': '25 мая'
        }
        
        # Известные даты занятий
        self.known_dates = ['18 мая', '22 мая', '25 мая']
    
    def connect(self):
        """Подключение к базе данных"""
        try:
            if self.db_type == 'sqlite':
                self.conn = sqlite3.connect(self.database)
                logger.info(f"Подключено к SQLite базе данных: {self.database}")
            elif self.db_type == 'postgresql':
                # Более подробное логирование параметров подключения
                logger.info(f"Пытаемся подключиться к PostgreSQL: host={self.host}, db={self.database}, user={self.user}, port={self.port}")
                
                # Подключение к PostgreSQL
                self.conn = psycopg2.connect(
                    host=self.host,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    port=self.port
                )
                
                # Проверка подключения
                test_cursor = self.conn.cursor()
                test_cursor.execute("SELECT current_database(), current_user")
                db_info = test_cursor.fetchone()
                test_cursor.close()
                logger.info(f"Успешно подключено к PostgreSQL. БД: {db_info[0]}, пользователь: {db_info[1]}")
            else:
                raise ValueError(f"Неподдерживаемый тип базы данных: {self.db_type}")
                
            self.cursor = self.conn.cursor()
            return True
        except Exception as e:
            import traceback
            logger.error(f"Ошибка подключения к базе данных: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def disconnect(self):
        """Закрытие соединения с базой данных"""
        if self.conn:
            if self.cursor:
                self.cursor.close()
            self.conn.close()
            logger.info("Соединение с базой данных закрыто")
    
    def get_video_downloads(self):
        """Получить данные о загрузках видео по датам
        
        Returns:
            dict: словарь {дата: [список пользователей]}
        """
        if not self.conn:
            if not self.connect():
                logger.error("Не удалось подключиться к базе данных")
                return {}
        
        result = {date: [] for date in self.known_dates}
        
        try:
            # Проверим структуру таблицы logs
            try:
                if self.db_type == 'postgresql':
                    self.cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'logs'")
                else:  # SQLite
                    self.cursor.execute("PRAGMA table_info(logs)")
                columns = self.cursor.fetchall()
                logger.info(f"Столбцы таблицы logs: {columns}")
                
                # Проверим количество записей
                self.cursor.execute("SELECT COUNT(*) FROM logs")
                count = self.cursor.fetchone()[0]
                logger.info(f"Всего записей в таблице logs: {count}")
                
                # Проверим типы действий
                self.cursor.execute("SELECT DISTINCT action FROM logs LIMIT 20")
                actions = self.cursor.fetchall()
                logger.info(f"Примеры действий: {actions}")
            except Exception as e:
                logger.error(f"Ошибка при проверке структуры базы данных: {e}")
            
            # Для каждой даты получаем список пользователей
            for date in self.known_dates:
                # Самый простой запрос, который работает в обоих типах баз данных
                query = """
                    SELECT DISTINCT l.username, l.user_id, u.first_name, u.last_name
                    FROM logs l
                    LEFT JOIN users u ON l.user_id = u.user_id
                    WHERE (l.action_data LIKE ? OR l.action LIKE ?)
                    ORDER BY l.username
                """
                
                params = [f'%{date}%', f'%{date}%']
                
                # Для PostgreSQL нужно заменить ? на %s
                if self.db_type == 'postgresql':
                    query = query.replace('?', '%s')
                
                logger.info(f"Запрос для даты {date}: {query}")
                logger.info(f"Параметры: {params}")
                
                try:
                    self.cursor.execute(query, params)
                    users = self.cursor.fetchall()
                    logger.info(f"Найдено пользователей для даты {date}: {len(users)}")
                
                    # Если ничего не нашли, попробуем еще один запрос
                    if len(users) == 0:
                        # Попробуем поискать по конкретным действиям
                        if date == '25 мая':
                            actions_to_check = ['get_video_25 мая', 'video_25']
                        elif date == '22 мая':
                            actions_to_check = ['get_video_22 мая', 'get_previous_video', 'video_22']
                        elif date == '18 мая':
                            actions_to_check = ['get_video_18 мая', 'get_latest_video', 'video_18']
                        else:
                            actions_to_check = []
                        
                        if actions_to_check:
                            placeholders = ', '.join(['%s' if self.db_type == 'postgresql' else '?'] * len(actions_to_check))
                            action_query = f"""
                                SELECT DISTINCT l.username, l.user_id, u.first_name, u.last_name
                                FROM logs l
                                LEFT JOIN users u ON l.user_id = u.user_id
                                WHERE l.action IN ({placeholders})
                                ORDER BY l.username
                            """
                            
                            logger.info(f"Дополнительный запрос для даты {date}: {action_query}")
                            logger.info(f"Параметры: {actions_to_check}")
                            
                            try:
                                self.cursor.execute(action_query, actions_to_check)
                                more_users = self.cursor.fetchall()
                                logger.info(f"Найдено дополнительных пользователей: {len(more_users)}")
                                users.extend(more_users)
                            except Exception as e:
                                logger.error(f"Ошибка в дополнительном запросе: {e}")
                
                    # Формируем список пользователей для этой даты
                    for user_row in users:
                        username, user_id, first_name, last_name = user_row
                        
                        # Ищем полное имя пользователя в нашем файле names.csv
                        full_name_from_csv = None
                        if username and self.names_loader:
                            full_name_from_csv = self.names_loader.get_full_name(username)
                            if full_name_from_csv:
                                logger.info(f"Найдено полное имя для @{username}: {full_name_from_csv}")
                        
                        # Формируем отображаемое имя
                        if username:
                            # Если есть полное имя из CSV, добавляем его к нику
                            if full_name_from_csv:
                                display_name = f"@{username} ({full_name_from_csv})"
                            else:
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
                            'display_name': display_name,
                            'full_name_from_csv': full_name_from_csv
                        }
                        
                        result[date].append(user_info)
                except Exception as e:
                    logger.error(f"Ошибка при выполнении запроса для даты {date}: {e}")
            
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
