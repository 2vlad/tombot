FROM python:3.9

WORKDIR /app

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование файлов проекта
COPY . .

# Создание директории для данных
RUN mkdir -p /app/data

# Запуск бота через start.py
CMD ["python", "start.py"]
