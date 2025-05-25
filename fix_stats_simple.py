#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re

def fix_stats_simple():
    """
    Replace the show_stats function with an extremely simplified version
    that uses only the simplest string formatting techniques
    """
    print("\n===== SIMPLE FIX FOR SHOW_STATS FUNCTION =====\n")
    
    # Path to bot.py file
    bot_py_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bot.py')
    
    # Check if file exists
    if not os.path.exists(bot_py_path):
        print(f"\nError: file {bot_py_path} not found")
        return
    
    try:
        # Create backup of bot.py
        backup_path = bot_py_path + '.simple_fix.bak'
        with open(bot_py_path, 'r', encoding='utf-8') as f_in, open(backup_path, 'w', encoding='utf-8') as f_out:
            f_out.write(f_in.read())
        
        print(f"  - Created backup at {backup_path}")
        
        # New implementation of the show_stats function with extremely simple string formatting
        new_function = '''
def show_stats(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        update.message.reply_text("У вас нет прав для выполнения этой команды.")
        return
    
    try:
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
        
        # Create list of users who started the bot
        cursor.execute("SELECT DISTINCT user_id FROM logs")
        active_users = set([row[0] for row in cursor.fetchall()])
        
        # Get count of unique users who accessed the latest video
        if db_type == 'postgres':
            cursor.execute("SELECT COUNT(DISTINCT COALESCE(username::text, user_id::text)) FROM logs WHERE action = 'get_latest_video'")
        else:
            cursor.execute("SELECT COUNT(DISTINCT COALESCE(username, user_id)) FROM logs WHERE action = 'get_latest_video'")
        latest_video_unique_users = cursor.fetchone()[0]
        
        # Get count of unique users who accessed the previous video
        if db_type == 'postgres':
            cursor.execute("SELECT COUNT(DISTINCT COALESCE(username::text, user_id::text)) FROM logs WHERE action = 'get_previous_video'")
        else:
            cursor.execute("SELECT COUNT(DISTINCT COALESCE(username, user_id)) FROM logs WHERE action = 'get_previous_video'")
        previous_video_unique_users = cursor.fetchone()[0]
        
        # Get list of administrators
        if db_type == 'postgres':
            cursor.execute("SELECT username, user_id FROM users WHERE is_admin = TRUE ORDER BY username")
        else:
            cursor.execute("SELECT username, user_id FROM users WHERE is_admin = 1 ORDER BY username")
        admins_list = cursor.fetchall()
        
        # Get list of users who clicked on the latest video button
        if db_type == 'postgres':
            cursor.execute("""
                SELECT DISTINCT username, user_id 
                FROM logs 
                WHERE action = 'get_latest_video' 
                ORDER BY username
            """)
        else:
            cursor.execute("""
                SELECT DISTINCT username, user_id 
                FROM logs 
                WHERE action = 'get_latest_video' 
                ORDER BY username
            """)
        latest_video_users = cursor.fetchall()
        
        # Get list of users who clicked on the previous video button
        if db_type == 'postgres':
            cursor.execute("""
                SELECT DISTINCT username, user_id 
                FROM logs 
                WHERE action = 'get_previous_video' 
                ORDER BY username
            """)
        else:
            cursor.execute("""
                SELECT DISTINCT username, user_id 
                FROM logs 
                WHERE action = 'get_previous_video' 
                ORDER BY username
            """)
        previous_video_users = cursor.fetchall()
        
        conn.close()
        
        # Format basic statistics with simple string concatenation
        stats_text = "*Статистика бота*\n\n"
        stats_text += "Всего пользователей: " + str(total_users) + "\n"
        stats_text += "Запустили бота: " + str(len(active_users)) + "\n"
        stats_text += "Администраторов: " + str(total_admins) + "\n\n"
        
        # If there are users who were added but didn't start the bot
        inactive_users = total_users - len(active_users)
        if inactive_users > 0:
            stats_text += "Добавлено, но не запустили бота: " + str(inactive_users) + "\n\n"
        
        # Add list of administrators
        stats_text += "*Список администраторов:*\n"
        for admin in admins_list:
            username, admin_id = admin
            admin_display = f"@{username}" if username else f"ID: {admin_id}"
            stats_text += f"- {admin_display}\n"
        stats_text += "\n"
        
        # Add statistics for latest video with list of users
        stats_text += f"*Запись занятия 18 мая получили: {latest_video_unique_users}*\n"
        for user in latest_video_users:
            username, user_id = user
            user_display = f"@{username}" if username else f"ID: {user_id}"
            stats_text += f"- {user_display}\n"
        stats_text += "\n"
        
        # Add statistics for previous video with list of users
        stats_text += f"*Запись занятия 22 мая получили: {previous_video_unique_users}*\n"
        for user in previous_video_users:
            username, user_id = user
            user_display = f"@{username}" if username else f"ID: {user_id}"
            stats_text += f"- {user_display}\n"
        
        # Send statistics
        update.message.reply_text(stats_text, parse_mode=ParseMode.MARKDOWN)
        log_action(user_id, 'show_stats', 'admin_command')
    except Exception as e:
        error_message = "Ошибка при получении статистики: " + str(e)
        update.message.reply_text(error_message)
        print("Error in show_stats: " + str(e))
'''
        
        # Read the file content
        with open(bot_py_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Replace the show_stats function
        pattern = re.compile(r'def show_stats\(update: Update, context: CallbackContext\) -> None:.*?(?=\n\ndef)', re.DOTALL)
        content = pattern.sub(new_function, content)
        
        # Write the updated content back to the file
        with open(bot_py_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print("  - Successfully replaced the show_stats function with a simplified version")
        print("  - The /stats command should now work without any syntax errors")
        
    except Exception as e:
        print(f"\nError fixing show_stats function: {e}")
    
    print("\n===== SIMPLE FIX FOR SHOW_STATS FUNCTION COMPLETED =====\n")

def main():
    fix_stats_simple()

if __name__ == "__main__":
    main()
