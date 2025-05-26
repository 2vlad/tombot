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
        """Возвращает полное имя по никнейму Telegram.
        
        Args:
            username (str): никнейм в Telegram (с @ или без)
            
        Returns:
            str: полное имя или None, если соответствие не найдено
        """
        if not username:
            return None
            
        # Нормализуем имя пользователя
        normalized_username = username.lower().strip()
        
        # Пробуем разные варианты поиска
        
        # 1. Прямое соответствие
        if normalized_username in self.telegram_to_name:
            return self.telegram_to_name[normalized_username]
        
        # 2. Без @ в начале, если он есть
        if normalized_username.startswith('@'):
            without_at = normalized_username[1:]
            if without_at in self.telegram_to_name:
                return self.telegram_to_name[without_at]
        
        # 3. С @ в начале, если его нет
        if not normalized_username.startswith('@'):
            with_at = '@' + normalized_username
            if with_at in self.telegram_to_name:
                return self.telegram_to_name[with_at]
                
        # 4. Проверяем на частичное совпадение
        clean_username = normalized_username.replace('@', '').strip()
        for telegram_name in self.telegram_to_name.keys():
            clean_telegram = telegram_name.replace('@', '').strip()
            if clean_username == clean_telegram:
                return self.telegram_to_name[telegram_name]
                
        # Ничего не нашли
        return None
