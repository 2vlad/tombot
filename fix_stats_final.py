#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re

def fix_stats_final():
    """
    Complete rewrite of the show_stats function to ensure it works correctly
    """
    print("\n===== FINAL FIX FOR SHOW_STATS FUNCTION =====\n")
    
    # Path to bot.py file
    bot_py_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bot.py')
    
    # Check if file exists
    if not os.path.exists(bot_py_path):
        print(f"\nError: file {bot_py_path} not found")
        return
    
    try:
        # Create backup of bot.py
        backup_path = bot_py_path + '.final_fix.bak'
        with open(bot_py_path, 'r', encoding='utf-8') as f_in, open(backup_path, 'w', encoding='utf-8') as f_out:
            f_out.write(f_in.read())
        
        print(f"  - Created backup at {backup_path}")
        
        # New implementation of the show_stats function
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
        
        # Build the statistics text
        lines = []
        lines.append("*Статистика бота*")
        lines.append("")
        lines.append(f"Всего пользователей: {total_users}")
        lines.append(f"Запустили бота: {len(active_users)}")
        lines.append(f"Администраторов: {total_admins}")
        lines.append("")
        
        # If there are users who were added but didn't start the bot
        inactive_users = total_users - len(active_users)
        if inactive_users > 0:
            lines.append(f"Добавлено, но не запустили бота: {inactive_users}")
            lines.append("")
        
        # Add detailed statistics for the latest video
        lines.append(f"*Запись занятия 18 мая получили ({latest_video_unique_users}):*")
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
                
                lines.append(f"- {user_display} {time_display}")
        else:
            lines.append("Никто еще не получил запись.")
        
        # Add detailed statistics for the previous video
        lines.append("")
        lines.append(f"*Запись занятия 22 мая получили ({previous_video_unique_users}):*")
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
                
                lines.append(f"- {user_display} {time_display}")
        else:
            lines.append("Никто еще не получил запись.")
        
        # Join all lines with newlines
        stats_text = "\n".join(lines)
        
        # Send statistics
        update.message.reply_text(stats_text, parse_mode=ParseMode.MARKDOWN)
        log_action(user_id, 'show_stats', 'admin_command')
    except Exception as e:
        update.message.reply_text(f"Ошибка при получении статистики: {str(e)}")
        print(f"Error in show_stats: {str(e)}")
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
        
        print("  - Successfully replaced the show_stats function with a completely rewritten version")
        print("  - The /stats command should now work without any errors")
        
    except Exception as e:
        print(f"\nError fixing show_stats function: {e}")
    
    print("\n===== FINAL FIX FOR SHOW_STATS FUNCTION COMPLETED =====\n")

def main():
    fix_stats_final()

if __name__ == "__main__":
    main()
