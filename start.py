#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import subprocess
import sys

# Запускаем скрипт настройки базы данных
print("Настраиваем базу данных...")
try:
    import db_setup
    db_setup.setup_database()
    print("База данных успешно настроена!")
except Exception as e:
    print(f"Ошибка при настройке базы данных: {e}")
    sys.exit(1)

# Запускаем скрипт исправления базы данных для Railway
print("Запускаем исправление базы данных для Railway...")
try:
    import railway_fix
    print("Исправление базы данных для Railway завершено!")
except Exception as e:
    print(f"Ошибка при исправлении базы данных для Railway: {e}")
    # Продолжаем выполнение, даже если возникла ошибка

# Запускаем бот
print("Запускаем бот...")
try:
    # Проверяем наличие необходимых переменных окружения
    telegram_token = os.environ.get('TELEGRAM_TOKEN')
    admin_id = os.environ.get('ADMIN_ID')
    
    if not telegram_token:
        print("Ошибка: Не указан токен Telegram (TELEGRAM_TOKEN)")
        sys.exit(1)
    
    if not admin_id:
        print("Предупреждение: Не указан ID администратора (ADMIN_ID)")
    
    # Запускаем бот
    subprocess.run([sys.executable, "bot.py"])
    
except Exception as e:
    print(f"Ошибка при запуске бота: {e}")
    sys.exit(1)
