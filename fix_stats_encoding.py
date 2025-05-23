#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re

def fix_stats_encoding():
    """
    Исправляет проблему с кодировкой в функции show_stats
    """
    print("\n===== ИСПРАВЛЕНИЕ ПРОБЛЕМЫ С КОДИРОВКОЙ В SHOW_STATS =====\n")
    
    # Путь к файлу bot.py
    bot_py_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bot.py')
    
    # Проверяем, существует ли файл
    if not os.path.exists(bot_py_path):
        print(f"\nОшибка: файл {bot_py_path} не найден")
        return
    
    try:
        # Создаем резервную копию bot.py
        backup_path = bot_py_path + '.encoding_fix.bak'
        with open(bot_py_path, 'r', encoding='utf-8') as f_in, open(backup_path, 'w', encoding='utf-8') as f_out:
            f_out.write(f_in.read())
        
        print(f"  - Создана резервная копия {backup_path}")
        
        # Напрямую заменяем функцию show_stats с использованием кириллицы
        new_function = '''
def show_stats(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text("У вас нет прав для выполнения этой команды.")
        return
    
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    # Total users
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]
    
    # Total admins
    if db_type == 'postgres':
        cursor.execute("SELECT COUNT(*) FROM users WHERE is_admin = TRUE")
    else:
        cursor.execute("SELECT COUNT(*) FROM users WHERE is_admin = 1")
    total_admins = cursor.fetchone()[0]
    
    # Count users who started the bot
    cursor.execute("""
    SELECT COUNT(DISTINCT user_id) FROM logs
    """)
    started_bot_count = cursor.fetchone()[0]
    
    # Get list of all users for display in stats
    cursor.execute("SELECT user_id, username, first_name, last_name FROM users")
    all_users = cursor.fetchall()
    
    # Create list of users who started the bot
    cursor.execute("SELECT DISTINCT user_id FROM logs")
    active_users = set([row[0] for row in cursor.fetchall()])
    
    # Video access count for each button
    cursor.execute("""
    SELECT COUNT(*) FROM logs 
    WHERE action = 'get_latest_video'
    """)
    latest_video_count = cursor.fetchone()[0]
    
    cursor.execute("""
    SELECT COUNT(*) FROM logs 
    WHERE action = 'get_previous_video'
    """)
    previous_video_count = cursor.fetchone()[0]
    
    # Get users who accessed the latest video with all their access timestamps
    if db_type == 'postgres':
        cursor.execute("""
        SELECT username, first_name, last_name, user_id, STRING_AGG(timestamp::text, ', ') as access_times
        FROM logs 
        WHERE action = 'get_latest_video'
        GROUP BY COALESCE(username, user_id), username, first_name, last_name, user_id
        ORDER BY MAX(timestamp) DESC
        """)
    else:
        cursor.execute("""
        SELECT username, first_name, last_name, user_id, GROUP_CONCAT(timestamp, ', ') as access_times
        FROM logs 
        WHERE action = 'get_latest_video'
        GROUP BY COALESCE(username, user_id)
        ORDER BY MAX(timestamp) DESC
        """)
    latest_video_users = cursor.fetchall()
    
    # Get count of unique users who accessed the latest video
    if db_type == 'postgres':
        cursor.execute("""
        SELECT COUNT(DISTINCT COALESCE(username::text, user_id::text)) 
        FROM logs 
        WHERE action = 'get_latest_video'
        """)
    else:
        cursor.execute("""
        SELECT COUNT(DISTINCT COALESCE(username, user_id)) 
        FROM logs 
        WHERE action = 'get_latest_video'
        """)
    latest_video_unique_users = cursor.fetchone()[0]
    
    # Get users who accessed the previous video with all their access timestamps
    if db_type == 'postgres':
        cursor.execute("""
        SELECT username, first_name, last_name, user_id, STRING_AGG(timestamp::text, ', ') as access_times
        FROM logs 
        WHERE action = 'get_previous_video'
        GROUP BY COALESCE(username, user_id), username, first_name, last_name, user_id
        ORDER BY MAX(timestamp) DESC
        """)
    else:
        cursor.execute("""
        SELECT username, first_name, last_name, user_id, GROUP_CONCAT(timestamp, ', ') as access_times
        FROM logs 
        WHERE action = 'get_previous_video'
        GROUP BY COALESCE(username, user_id)
        ORDER BY MAX(timestamp) DESC
        """)
    previous_video_users = cursor.fetchall()
    
    # Get count of unique users who accessed the previous video
    if db_type == 'postgres':
        cursor.execute("""
        SELECT COUNT(DISTINCT COALESCE(username::text, user_id::text)) 
        FROM logs 
        WHERE action = 'get_previous_video'
        """)
    else:
        cursor.execute("""
        SELECT COUNT(DISTINCT COALESCE(username, user_id)) 
        FROM logs 
        WHERE action = 'get_previous_video'
        """)
    previous_video_unique_users = cursor.fetchone()[0]
    
    conn.close()
    
    # Format basic statistics
    stats_text = "*Статистика бота*\n\n"
    stats_text += f"Всего пользователей: {total_users}\n"
    stats_text += f"Запустили бота: {len(active_users)}\n"
    stats_text += f"Администраторов: {total_admins}\n\n"
    
    # If there are users who were added but didn't start the bot
    inactive_users = total_users - len(active_users)
    if inactive_users > 0:
        stats_text += f"Добавлено, но не запустили бота: {inactive_users}\n\n"
    
    # Add detailed statistics for the latest video
    stats_text += f"*Запись занятия 18 мая получили ({latest_video_unique_users}):*\n"
    if latest_video_users:
        for user in latest_video_users:
            username, first_name, last_name, user_id, access_times = user
            # If there's a username, use it, otherwise use user_id
            if username:
                user_display = f"@{username}"
            else:
                user_display = f"ID: {user_id}"
                
            if first_name or last_name:
                name = f"{first_name or ''} {last_name or ''}" .strip()
                if username:
                    user_display += f" ({name})"
                else:
                    user_display = f"{name} ({user_display})"
            elif not username:
                user_display = f"Пользователь {user_display}"
            
            # Format access times to show all timestamps when user accessed the video
            timestamps = access_times.split(', ')
            if len(timestamps) > 1:
                # Limit the number of displayed timestamps to avoid issues with Markdown
                if len(timestamps) > 3:
                    time_display = f"({timestamps[0]}, {timestamps[1]}, ... и еще {len(timestamps)-2})"
                else:
                    time_display = f"({timestamps[0]}"
                    for ts in timestamps[1:]:
                        time_display += f", {ts}"
                    time_display += ")"
            else:
                time_display = f"({access_times})"
            
            stats_text += f"- {user_display} {time_display}\n"
    else:
        stats_text += "Никто еще не получил запись.\n"
    
    # Add detailed statistics for the previous video
    stats_text += f"\n*Запись занятия 22 мая получили ({previous_video_unique_users}):*\n"
    if previous_video_users:
        for user in previous_video_users:
            username, first_name, last_name, user_id, access_times = user
            # If there's a username, use it, otherwise use user_id
            if username:
                user_display = f"@{username}"
            else:
                user_display = f"ID: {user_id}"
                
            if first_name or last_name:
                name = f"{first_name or ''} {last_name or ''}" .strip()
                if username:
                    user_display += f" ({name})"
                else:
                    user_display = f"{name} ({user_display})"
            elif not username:
                user_display = f"Пользователь {user_display}"
            
            # Format access times to show all timestamps when user accessed the video
            timestamps = access_times.split(', ')
            if len(timestamps) > 1:
                # Limit the number of displayed timestamps to avoid issues with Markdown
                if len(timestamps) > 3:
                    time_display = f"({timestamps[0]}, {timestamps[1]}, ... и еще {len(timestamps)-2})"
                else:
                    time_display = f"({timestamps[0]}"
                    for ts in timestamps[1:]:
                        time_display += f", {ts}"
                    time_display += ")"
            else:
                time_display = f"({access_times})"
            
            stats_text += f"- {user_display} {time_display}\n"
    else:
        stats_text += "Никто еще не получил запись.\n"
    
    # Send statistics
    update.message.reply_text(stats_text, parse_mode=ParseMode.MARKDOWN)
    log_action(user_id, 'show_stats', 'admin_command')
'''
        
        # Читаем файл bot.py
        with open(bot_py_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Заменяем функцию show_stats
        pattern = re.compile(r'def show_stats\(update: Update, context: CallbackContext\) -> None:.*?(?=\n\ndef)', re.DOTALL)
        content = pattern.sub(new_function, content)
        
        # Записываем изменения в файл
        with open(bot_py_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print("  - Функция show_stats успешно заменена с использованием обычных кириллических символов")
        print("  - Теперь команда /stats должна работать без ошибок и отображаться корректно")
        
    except Exception as e:
        print(f"\nОшибка при исправлении кодировки: {e}")
    
    print("\n===== ИСПРАВЛЕНИЕ ПРОБЛЕМЫ С КОДИРОВКОЙ ЗАВЕРШЕНО =====\n")

def main():
    fix_stats_encoding()

if __name__ == "__main__":
    main()
