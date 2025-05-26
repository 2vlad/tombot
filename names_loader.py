import csv
import logging

logger = logging.getLogger(__name__)

class NamesLoader:
    """
    Класс для загрузки соответствия между никнеймами Telegram и полными именами пользователей из CSV-файла.
    """
    def __init__(self, file_path='names.csv'):
        self.file_path = file_path
        self.telegram_to_name = {}
        self.load_names()
    
    def load_names(self):
        """
        Загружает данные из CSV-файла в словарь telegram_to_name.
        """
        try:
            with open(self.file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                # Пропускаем первые две строки (заголовки)
                next(reader, None)
                next(reader, None)
                
                # Обрабатываем данные
                for row in reader:
                    if len(row) >= 3:
                        # Получаем имя и ник в Telegram
                        full_name = row[1].strip()
                        telegram = row[2].strip()
                        
                        # Если ник начинается с @, используем его, иначе пропускаем
                        if telegram.startswith('@'):
                            # Удаляем @ для сравнения
                            username = telegram[1:].strip().lower()
                            self.telegram_to_name[username] = full_name
                            logger.debug(f"Загружено соответствие: {username} -> {full_name}")
                            
            logger.info(f"Загружено {len(self.telegram_to_name)} соответствий имен")
        except Exception as e:
            logger.error(f"Ошибка при загрузке файла имен: {e}")
    
    def get_full_name(self, username):
        """
        Возвращает полное имя по никнейму Telegram.
        
        Args:
            username (str): никнейм в Telegram (без @)
            
        Returns:
            str: полное имя или None, если соответствие не найдено
        """
        if not username:
            return None
            
        # Нормализуем имя пользователя
        username = username.lower().strip()
        
        return self.telegram_to_name.get(username)
