import os
import sqlite3
import psycopg2
import dj_database_url
from dotenv import load_dotenv
import logging

# Загружаем переменные окружения из .env файла, если он существует
load_dotenv()

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Константы для типов баз данных
DB_TYPE_SQLITE = 'sqlite'
DB_TYPE_POSTGRES = 'postgres'

def get_db_connection():
    """
    Возвращает соединение с базой данных в зависимости от окружения.
    В Railway используется PostgreSQL, локально - SQLite.
    """
    # Проверяем наличие переменной окружения DATABASE_URL (устанавливается Railway)
    database_url = os.environ.get('DATABASE_URL')
    
    if database_url:
        # Используем PostgreSQL на Railway
        try:
            # Парсим URL базы данных
            config = dj_database_url.parse(database_url)
            
            # Создаем соединение с PostgreSQL
            conn = psycopg2.connect(
                dbname=config['NAME'],
                user=config['USER'],
                password=config['PASSWORD'],
                host=config['HOST'],
                port=config['PORT']
            )
            logger.info("Connected to PostgreSQL database")
            return conn, DB_TYPE_POSTGRES
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            # Если не удалось подключиться к PostgreSQL, используем SQLite как запасной вариант
            logger.info("Falling back to SQLite database")
    
    # Используем SQLite локально или как запасной вариант
    db_path = get_sqlite_path()
    conn = sqlite3.connect(db_path)
    logger.info(f"Connected to SQLite database at {db_path}")
    return conn, DB_TYPE_SQLITE

def get_sqlite_path():
    """
    Определяет путь к файлу базы данных SQLite.
    Учитывает возможность использования тома Railway.
    """
    # Проверяем, есть ли переменная окружения RAILWAY_VOLUME_MOUNT_PATH
    volume_path = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH')
    
    if volume_path:
        # Если мы на Railway с подключенным томом
        import pathlib
        db_dir = pathlib.Path(volume_path)
        # Создаем директорию, если она не существует
        db_dir.mkdir(parents=True, exist_ok=True)
        return str(db_dir / 'filmschool.db')
    else:
        # Локальная разработка
        return 'filmschool.db'

def setup_database():
    """
    Создает необходимые таблицы в базе данных, если они не существуют.
    Поддерживает как SQLite, так и PostgreSQL.
    """
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    # Создаем таблицы с учетом синтаксических различий между SQLite и PostgreSQL
    if db_type == DB_TYPE_POSTGRES:
        # PostgreSQL синтаксис
        # Таблица пользователей
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            registration_date TIMESTAMP,
            is_admin BOOLEAN DEFAULT FALSE
        )
        """)
        
        # Таблица логов
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            action TEXT,
            action_data TEXT,
            timestamp TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (user_id)
        )
        """)
        
        # Таблица ожидающих пользователей
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pending_users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            request_date TIMESTAMP
        )
        """)
        
        # Таблица кнопок
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS buttons (
            id SERIAL PRIMARY KEY,
            button_key VARCHAR(255) UNIQUE,
            button_text VARCHAR(255),
            button_url TEXT,
            last_updated TIMESTAMP
        )
        """)
    else:
        # SQLite синтаксис
        # Таблица пользователей
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            registration_date TEXT,
            is_admin INTEGER DEFAULT 0
        )
        """)
        
        # Таблица логов
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS logs (
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
        """)
        
        # Таблица ожидающих пользователей
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pending_users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            request_date TEXT
        )
        """)
        
        # Таблица кнопок
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS buttons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            button_key TEXT UNIQUE,
            button_text TEXT,
            button_url TEXT,
            last_updated TEXT
        )
        """)
    
    # Вставляем администратора по умолчанию, если он не существует
    admin_id = os.environ.get('ADMIN_ID', None)
    if admin_id:
        if db_type == DB_TYPE_POSTGRES:
            cursor.execute(
                "INSERT INTO users (user_id, is_admin) VALUES (%s, %s) ON CONFLICT (user_id) DO NOTHING",
                (int(admin_id), True)
            )
        else:
            cursor.execute(
                "INSERT OR IGNORE INTO users (user_id, is_admin) VALUES (?, ?)",
                (int(admin_id), 1)
            )
    
    conn.commit()
    conn.close()

def load_buttons():
    """
    Загружает настройки кнопок из базы данных.
    Возвращает словарь с настройками кнопок.
    """
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    buttons = {}
    
    # Проверяем, есть ли кнопки в базе данных
    try:
        if db_type == DB_TYPE_POSTGRES:
            cursor.execute("SELECT button_key, button_text, button_url FROM buttons")
        else:
            cursor.execute("SELECT button_key, button_text, button_url FROM buttons")
        
        for row in cursor.fetchall():
            button_key, button_text, button_url = row
            # Преобразуем button_key в номер кнопки (например, 'button1' -> 1)
            if button_key.startswith('button') and button_key[6:].isdigit():
                button_number = int(button_key[6:])
                buttons[button_number] = {'text': button_text, 'message': button_url}
    except Exception as e:
        print(f"Error loading buttons: {e}")
    
    conn.close()
    return buttons

def save_button(button_number, button_text, button_url):
    """
    Сохраняет настройки кнопки в базу данных.
    """
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    # Текущая дата и время
    if db_type == DB_TYPE_POSTGRES:
        # PostgreSQL использует NOW() для текущей даты и времени
        now = "NOW()"
        
        # Проверяем, существует ли кнопка
        cursor.execute("SELECT id FROM buttons WHERE button_key = %s", (f'button{button_number}',))
        button_id = cursor.fetchone()
        
        if button_id:
            # Обновляем существующую кнопку
            cursor.execute(
                "UPDATE buttons SET button_text = %s, button_url = %s, last_updated = " + now + " WHERE button_key = %s",
                (button_text, button_url, f'button{button_number}')
            )
        else:
            # Вставляем новую кнопку
            cursor.execute(
                "INSERT INTO buttons (button_key, button_text, button_url, last_updated) VALUES (%s, %s, %s, " + now + ")",
                (f'button{button_number}', button_text, button_url)
            )
    else:
        # SQLite использует datetime('now') для текущей даты и времени
        import datetime
        import pytz
        now = datetime.datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
        
        # Проверяем, существует ли кнопка
        cursor.execute("SELECT id FROM buttons WHERE button_key = ?", (f'button{button_number}',))
        button_id = cursor.fetchone()
        
        if button_id:
            # Обновляем существующую кнопку
            cursor.execute(
                "UPDATE buttons SET button_text = ?, button_url = ?, last_updated = ? WHERE button_key = ?",
                (button_text, button_url, now, f'button{button_number}')
            )
        else:
            # Вставляем новую кнопку
            cursor.execute(
                "INSERT INTO buttons (button_key, button_text, button_url, last_updated) VALUES (?, ?, ?, ?)",
                (f'button{button_number}', button_text, button_url, now)
            )
    
    conn.commit()
    conn.close()
