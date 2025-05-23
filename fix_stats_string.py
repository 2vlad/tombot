#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re

def fix_stats_string():
    """
    u0418u0441u043fu0440u0430u0432u043bu044fu0435u0442 u043au043eu043du043au0440u0435u0442u043du0443u044e u0441u0442u0440u043eu043au0443 u0432 u0444u0443u043du043au0446u0438u0438 show_stats
    """
    print("\n===== u0418u0421u041fu0420u0410u0412u041bu0415u041du0418u0415 u041au041eu041du041au0420u0415u0422u041du041eu0419 u0421u0422u0420u041eu041au0418 u0412 SHOW_STATS =====\n")
    
    # u041fu0443u0442u044c u043a u0444u0430u0439u043bu0443 bot.py
    bot_py_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bot.py')
    
    # u041fu0440u043eu0432u0435u0440u044fu0435u043c, u0441u0443u0449u0435u0441u0442u0432u0443u0435u0442 u043bu0438 u0444u0430u0439u043b
    if not os.path.exists(bot_py_path):
        print(f"\nu041eu0448u0438u0431u043au0430: u0444u0430u0439u043b {bot_py_path} u043du0435 u043du0430u0439u0434u0435u043d")
        return
    
    try:
        # u0421u043eu0437u0434u0430u0435u043c u0440u0435u0437u0435u0440u0432u043du0443u044e u043au043eu043fu0438u044e bot.py
        backup_path = bot_py_path + '.string_fix.bak'
        with open(bot_py_path, 'r', encoding='utf-8') as f_in, open(backup_path, 'w', encoding='utf-8') as f_out:
            f_out.write(f_in.read())
        
        print(f"  - u0421u043eu0437u0434u0430u043du0430 u0440u0435u0437u0435u0440u0432u043du0430u044f u043au043eu043fu0438u044f {backup_path}")
        
        # u041du0430u043fu0440u044fu043cu0443u044e u0437u0430u043cu0435u043du0438u043c u0444u0443u043du043au0446u0438u044e show_stats
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
        
        # u0427u0438u0442u0430u0435u043c u0444u0430u0439u043b bot.py
        with open(bot_py_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # u0417u0430u043cu0435u043du044fu0435u043c u0444u0443u043du043au0446u0438u044e show_stats
        pattern = re.compile(r'def show_stats\(update: Update, context: CallbackContext\) -> None:.*?(?=\n\ndef)', re.DOTALL)
        content = pattern.sub(new_function, content)
        
        # u0417u0430u043fu0438u0441u044bu0432u0430u0435u043c u0438u0437u043cu0435u043du0435u043du0438u044f u0432 u0444u0430u0439u043b
        with open(bot_py_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print("  - u0424u0443u043du043au0446u0438u044f show_stats u0443u0441u043fu0435u0448u043du043e u0437u0430u043cu0435u043du0435u043du0430 u0441 u0438u0441u043fu0440u0430u0432u043bu0435u043du043du044bu043cu0438 u0441u0442u0440u043eu043au0430u043cu0438")
        print("  - u0422u0435u043fu0435u0440u044c u043au043eu043cu0430u043du0434u0430 /stats u0434u043eu043bu0436u043du0430 u0440u0430u0431u043eu0442u0430u0442u044c u0431u0435u0437 u043eu0448u0438u0431u043eu043a")
        
    except Exception as e:
        print(f"\nu041eu0448u0438u0431u043au0430 u043fu0440u0438 u0438u0441u043fu0440u0430u0432u043bu0435u043du0438u0438 u0441u0442u0440u043eu043au0438: {e}")
    
    print("\n===== u0418u0421u041fu0420u0410u0412u041bu0415u041du0418u0415 u041au041eu041du041au0420u0415u0422u041du041eu0419 u0421u0422u0420u041eu041au0418 u0417u0410u0412u0415u0420u0428u0415u041du041e =====\n")

def main():
    fix_stats_string()

if __name__ == "__main__":
    main()
