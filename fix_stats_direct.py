#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

def fix_stats_direct():
    """
    Directly fix the show_stats function by writing a completely new implementation
    with extremely basic string handling
    """
    print("\n===== DIRECT FIX FOR SHOW_STATS FUNCTION =====\n")
    
    # Path to bot.py file
    bot_py_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bot.py')
    
    # Check if file exists
    if not os.path.exists(bot_py_path):
        print(f"\nError: file {bot_py_path} not found")
        return
    
    try:
        # Create backup of bot.py
        backup_path = bot_py_path + '.direct_fix.bak'
        with open(bot_py_path, 'r', encoding='utf-8') as f_in, open(backup_path, 'w', encoding='utf-8') as f_out:
            f_out.write(f_in.read())
        
        print(f"  - Created backup at {backup_path}")
        
        # Read the file content
        with open(bot_py_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Find the start of the show_stats function
        start_index = -1
        for i, line in enumerate(lines):
            if line.strip().startswith('def show_stats(update: Update, context: CallbackContext)'):
                start_index = i
                break
        
        if start_index == -1:
            print("  - Could not find the show_stats function")
            return
        
        # Find the end of the show_stats function
        end_index = -1
        for i in range(start_index + 1, len(lines)):
            if lines[i].strip().startswith('def '):
                end_index = i
                break
        
        if end_index == -1:
            print("  - Could not find the end of the show_stats function")
            return
        
        # Create a new implementation of the show_stats function
        new_function_lines = [
            'def show_stats(update: Update, context: CallbackContext) -> None:\n',
            '    user_id = update.effective_user.id\n',
            '    \n',
            '    if not is_admin(user_id):\n',
            '        update.message.reply_text("У вас нет прав для выполнения этой команды.")\n',
            '        return\n',
            '    \n',
            '    try:\n',
            '        conn, db_type = get_db_connection()\n',
            '        cursor = conn.cursor()\n',
            '        \n',
            '        # Total users\n',
            '        cursor.execute("SELECT COUNT(*) FROM users")\n',
            '        total_users = cursor.fetchone()[0]\n',
            '        \n',
            '        # Total admins\n',
            '        if db_type == \'postgres\':\n',
            '            cursor.execute("SELECT COUNT(*) FROM users WHERE is_admin = TRUE")\n',
            '        else:\n',
            '            cursor.execute("SELECT COUNT(*) FROM users WHERE is_admin = 1")\n',
            '        total_admins = cursor.fetchone()[0]\n',
            '        \n',
            '        # Create list of users who started the bot\n',
            '        cursor.execute("SELECT DISTINCT user_id FROM logs")\n',
            '        active_users = set([row[0] for row in cursor.fetchall()])\n',
            '        \n',
            '        # Get count of unique users who accessed the latest video\n',
            '        if db_type == \'postgres\':\n',
            '            cursor.execute("SELECT COUNT(DISTINCT COALESCE(username::text, user_id::text)) FROM logs WHERE action = \'get_latest_video\'")\n',
            '        else:\n',
            '            cursor.execute("SELECT COUNT(DISTINCT COALESCE(username, user_id)) FROM logs WHERE action = \'get_latest_video\'")\n',
            '        latest_video_unique_users = cursor.fetchone()[0]\n',
            '        \n',
            '        # Get count of unique users who accessed the previous video\n',
            '        if db_type == \'postgres\':\n',
            '            cursor.execute("SELECT COUNT(DISTINCT COALESCE(username::text, user_id::text)) FROM logs WHERE action = \'get_previous_video\'")\n',
            '        else:\n',
            '            cursor.execute("SELECT COUNT(DISTINCT COALESCE(username, user_id)) FROM logs WHERE action = \'get_previous_video\'")\n',
            '        previous_video_unique_users = cursor.fetchone()[0]\n',
            '        \n',
            '        conn.close()\n',
            '        \n',
            '        # Format basic statistics with simple string concatenation\n',
            '        stats_text = "*Статистика бота*" + "\\n\\n"\n',
            '        stats_text += "Всего пользователей: " + str(total_users) + "\\n"\n',
            '        stats_text += "Запустили бота: " + str(len(active_users)) + "\\n"\n',
            '        stats_text += "Администраторов: " + str(total_admins) + "\\n\\n"\n',
            '        \n',
            '        # If there are users who were added but didn\'t start the bot\n',
            '        inactive_users = total_users - len(active_users)\n',
            '        if inactive_users > 0:\n',
            '            stats_text += "Добавлено, но не запустили бота: " + str(inactive_users) + "\\n\\n"\n',
            '        \n',
            '        # Add simplified statistics for videos\n',
            '        stats_text += "*Запись занятия 18 мая получили: " + str(latest_video_unique_users) + "*\\n\\n"\n',
            '        stats_text += "*Запись занятия 22 мая получили: " + str(previous_video_unique_users) + "*\\n"\n',
            '        \n',
            '        # Send statistics\n',
            '        update.message.reply_text(stats_text, parse_mode=ParseMode.MARKDOWN)\n',
            '        log_action(user_id, \'show_stats\', \'admin_command\')\n',
            '    except Exception as e:\n',
            '        error_message = "Ошибка при получении статистики: " + str(e)\n',
            '        update.message.reply_text(error_message)\n',
            '        print("Error in show_stats: " + str(e))\n',
            '\n'
        ]
        
        # Replace the show_stats function in the file
        new_lines = lines[:start_index] + new_function_lines + lines[end_index:]
        
        # Write the updated content back to the file
        with open(bot_py_path, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
        
        print("  - Successfully replaced the show_stats function with a direct implementation")
        print("  - The /stats command should now work without any syntax errors")
        
    except Exception as e:
        print(f"\nError fixing show_stats function: {e}")
    
    print("\n===== DIRECT FIX FOR SHOW_STATS FUNCTION COMPLETED =====\n")

def main():
    fix_stats_direct()

if __name__ == "__main__":
    main()
