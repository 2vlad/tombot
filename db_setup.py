import os
import shutil
import sqlite3
from pathlib import Path

# Определяем пути к файлам базы данных
DATA_DIR = Path("/app/data")
DB_FILE = "filmschool.db"
ORIGINAL_DB_PATH = Path(DB_FILE)
PERSISTENT_DB_PATH = DATA_DIR / DB_FILE

# Создаем директорию для данных, если она не существует
DATA_DIR.mkdir(exist_ok=True)

# Проверяем, существует ли база данных в постоянном хранилище
def setup_database():
    print("Настраиваем базу данных...")
    
    # Если база данных уже существует в постоянном хранилище, используем её
    if PERSISTENT_DB_PATH.exists():
        print(f"База данных найдена в {PERSISTENT_DB_PATH}")
        
        # Если в корневой директории тоже есть база данных, создаем резервную копию
        if ORIGINAL_DB_PATH.exists():
            backup_path = ORIGINAL_DB_PATH.with_suffix(".db.backup")
            print(f"Создаем резервную копию локальной базы данных: {backup_path}")
            shutil.copy2(ORIGINAL_DB_PATH, backup_path)
        
        # Создаем символическую ссылку на базу данных в постоянном хранилище
        print(f"Создаем символическую ссылку на базу данных в постоянном хранилище")
        if ORIGINAL_DB_PATH.exists():
            ORIGINAL_DB_PATH.unlink()
        os.symlink(PERSISTENT_DB_PATH, ORIGINAL_DB_PATH)
        
    # Если базы данных нет в постоянном хранилище, но есть в корневой директории
    elif ORIGINAL_DB_PATH.exists():
        print(f"Копируем базу данных в постоянное хранилище: {PERSISTENT_DB_PATH}")
        shutil.copy2(ORIGINAL_DB_PATH, PERSISTENT_DB_PATH)
        
        # Удаляем оригинальный файл и создаем символическую ссылку
        ORIGINAL_DB_PATH.unlink()
        os.symlink(PERSISTENT_DB_PATH, ORIGINAL_DB_PATH)
    
    # Если базы данных нет нигде, создаем пустую в постоянном хранилище
    else:
        print(f"Создаем новую базу данных в постоянном хранилище: {PERSISTENT_DB_PATH}")
        conn = sqlite3.connect(str(PERSISTENT_DB_PATH))
        conn.close()
        
        # Создаем символическую ссылку
        os.symlink(PERSISTENT_DB_PATH, ORIGINAL_DB_PATH)
    
    print("Настройка базы данных завершена.")

if __name__ == "__main__":
    setup_database()
