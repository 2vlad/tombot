#!/bin/bash

# Останавливаем все запущенные экземпляры бота
pkill -f "python3 bot.py" || true

# Устанавливаем переменные окружения
export TELEGRAM_TOKEN="7937927576:AAHVQm4AGYNWG6BD-ZNWSfn9XpAb-9wU1dw"
export ADMIN_ID="89118240"

# Запускаем бот
python3 bot.py
