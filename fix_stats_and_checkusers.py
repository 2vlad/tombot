#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import psycopg2
import sqlite3
import re

# u041fu043eu0434u043au043bu044eu0447u0430u0435u043cu0441u044f u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445
def get_db_connection():
    # u041fu0440u043eu0432u0435u0440u044fu0435u043c, u0435u0441u0442u044c u043bu0438 u043fu0435u0440u0435u043cu0435u043du043du0430u044f u043eu043au0440u0443u0436u0435u043du0438u044f DATABASE_URL (u0438u0441u043fu043eu043bu044cu0437u0443u0435u0442u0441u044f u0432 Railway)
    database_url = os.environ.get('DATABASE_URL')
    
    if database_url and database_url.startswith('postgres'):
        # u041fu043eu0434u043au043bu044eu0447u0435u043du0438u0435 u043a PostgreSQL
        print("Connected to PostgreSQL database")
        conn = psycopg2.connect(database_url)
        return conn, 'postgres'
    else:
        # u041fu043eu0434u043au043bu044eu0447u0435u043du0438u0435 u043a SQLite
        print("Connected to SQLite database at filmschool.db")
        conn = sqlite3.connect('filmschool.db')
        return conn, 'sqlite'

def fix_stats_and_checkusers():
    """
    u0418u0441u043fu0440u0430u0432u043bu044fu0435u0442 u043au043eu043cu0430u043du0434u044b /stats u0438 /checkusers
    """
    print("\n===== u0418u0421u041fu0420u0410u0412u041bu0415u041du0418u0415 u041au041eu041cu0410u041du0414 /stats u0418 /checkusers =====\n")
    
    # u041fu0443u0442u044c u043a u0444u0430u0439u043bu0443 bot.py
    bot_py_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bot.py')
    
    # u041fu0440u043eu0432u0435u0440u044fu0435u043c, u0441u0443u0449u0435u0441u0442u0432u0443u0435u0442 u043bu0438 u0444u0430u0439u043b
    if not os.path.exists(bot_py_path):
        print(f"\nu041eu0448u0438u0431u043au0430: u0444u0430u0439u043b {bot_py_path} u043du0435 u043du0430u0439u0434u0435u043d")
        return
    
    # u041fu043eu0434u043au043bu044eu0447u0430u0435u043cu0441u044f u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445 u0434u043bu044f u0442u0435u0441u0442u0438u0440u043eu0432u0430u043du0438u044f
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # u0421u043eu0437u0434u0430u0435u043c u0440u0435u0437u0435u0440u0432u043du0443u044e u043au043eu043fu0438u044e bot.py
        backup_path = bot_py_path + '.stats_checkusers.bak'
        with open(bot_py_path, 'r', encoding='utf-8') as f_in, open(backup_path, 'w', encoding='utf-8') as f_out:
            f_out.write(f_in.read())
        
        print(f"  - u0421u043eu0437u0434u0430u043du0430 u0440u0435u0437u0435u0440u0432u043du0430u044f u043au043eu043fu0438u044f {backup_path}")
        
        # u0427u0438u0442u0430u0435u043c u0444u0430u0439u043b bot.py
        with open(bot_py_path, 'r', encoding='utf-8') as f:
            bot_py_content = f.read()
        
        # 1. u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0444u0443u043du043au0446u0438u044e show_stats
        print("\n1. u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0444u0443u043du043au0446u0438u044e show_stats...")
        
        # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u043fu043eu0434u043au043bu044eu0447u0435u043du0438u0435 u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445 u0432 u0444u0443u043du043au0446u0438u0438 show_stats
        old_code_stats_1 = "conn = sqlite3.connect('filmschool.db')\n    cursor = conn.cursor()"
        new_code_stats_1 = "conn, db_type = get_db_connection()\n    cursor = conn.cursor()"
        
        # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c SQL-u0437u0430u043fu0440u043eu0441u044b u0432 u0444u0443u043du043au0446u0438u0438 show_stats
        old_code_stats_2 = """    # Total admins
    cursor.execute("SELECT COUNT(*) FROM users WHERE is_admin = 1")
    total_admins = cursor.fetchone()[0]"""
        
        new_code_stats_2 = """    # Total admins
    if db_type == 'postgres':
        cursor.execute("SELECT COUNT(*) FROM users WHERE is_admin = TRUE")
    else:
        cursor.execute("SELECT COUNT(*) FROM users WHERE is_admin = 1")
    total_admins = cursor.fetchone()[0]"""
        
        # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c GROUP_CONCAT u043du0430 PostgreSQL-u0441u043eu0432u043cu0435u0441u0442u0438u043cu044bu0439 u0432u0430u0440u0438u0430u043du0442
        old_code_stats_3 = """    # Get users who accessed the latest video with all their access timestamps
    cursor.execute("""
    SELECT username, first_name, last_name, user_id, GROUP_CONCAT(timestamp, ', ') as access_times
    FROM logs 
    WHERE action = 'get_latest_video'
    GROUP BY COALESCE(username, user_id)
    ORDER BY MAX(timestamp) DESC
    """)
    latest_video_users = cursor.fetchall()"""
        
        new_code_stats_3 = """    # Get users who accessed the latest video with all their access timestamps
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
    latest_video_users = cursor.fetchall()"""
        
        old_code_stats_4 = """    # Get users who accessed the previous video with all their access timestamps
    cursor.execute("""
    SELECT username, first_name, last_name, user_id, GROUP_CONCAT(timestamp, ', ') as access_times
    FROM logs 
    WHERE action = 'get_previous_video'
    GROUP BY COALESCE(username, user_id)
    ORDER BY MAX(timestamp) DESC
    """)
    previous_video_users = cursor.fetchall()"""
        
        new_code_stats_4 = """    # Get users who accessed the previous video with all their access timestamps
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
    previous_video_users = cursor.fetchall()"""
        
        # 2. u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0444u0443u043du043au0446u0438u044e check_users
        print("2. u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0444u0443u043du043au0446u0438u044e check_users...")
        
        # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0437u0430u043fu0440u043eu0441 u043a u0442u0430u0431u043bu0438u0446u0435 pending_users
        old_code_check_1 = """        # u041fu0440u043eu0432u0435u0440u044fu0435u043c, u0435u0441u0442u044c u043bu0438 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044c u0432 u0442u0430u0431u043bu0438u0446u0435 pending_users
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM pending_users WHERE username = %s", (username,))
        else:
            cursor.execute("SELECT user_id FROM pending_users WHERE username = ?", (username,))"""
        
        new_code_check_1 = """        # u041fu0440u043eu0432u0435u0440u044fu0435u043c, u0435u0441u0442u044c u043bu0438 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044c u0432 u0442u0430u0431u043bu0438u0446u0435 pending_users
        if db_type == 'postgres':
            cursor.execute("SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(%s) OR LOWER(username) = LOWER('@' || %s)", (username, username))
        else:
            cursor.execute("SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(?) OR LOWER(username) = LOWER('@' || ?)", (username, username))"""
        
        # u0417u0430u043cu0435u043du044fu0435u043c u043au043eu0434
        bot_py_content = bot_py_content.replace(old_code_stats_1, new_code_stats_1)
        bot_py_content = bot_py_content.replace(old_code_stats_2, new_code_stats_2)
        bot_py_content = bot_py_content.replace(old_code_stats_3, new_code_stats_3)
        bot_py_content = bot_py_content.replace(old_code_stats_4, new_code_stats_4)
        bot_py_content = bot_py_content.replace(old_code_check_1, new_code_check_1)
        
        # u0417u0430u043fu0438u0441u044bu0432u0430u0435u043c u0438u0437u043cu0435u043du0435u043du0438u044f u0432 u0444u0430u0439u043b
        with open(bot_py_path, 'w', encoding='utf-8') as f:
            f.write(bot_py_content)
        
        print("  - u0424u0443u043du043au0446u0438u0438 show_stats u0438 check_users u0443u0441u043fu0435u0448u043du043e u0438u0441u043fu0440u0430u0432u043bu0435u043du044b")
        print("  - u0422u0435u043fu0435u0440u044c u043au043eu043cu0430u043du0434u044b /stats u0438 /checkusers u0431u0443u0434u0443u0442 u0440u0430u0431u043eu0442u0430u0442u044c u043au043eu0440u0440u0435u043au0442u043du043e u0441 PostgreSQL")
        
    except Exception as e:
        print(f"\nu041eu0448u0438u0431u043au0430 u043fu0440u0438 u0438u0441u043fu0440u0430u0432u043bu0435u043du0438u0438 u043au043eu043cu0430u043du0434 /stats u0438 /checkusers: {e}")
    
    conn.close()
    print("\n===== u0418u0421u041fu0420u0410u0412u041bu0415u041du0418u0415 u041au041eu041cu0410u041du0414 /stats u0418 /checkusers u0417u0410u0412u0415u0420u0428u0415u041du041e =====\n")

def main():
    # u041fu043eu043bu0443u0447u0430u0435u043c u0441u0442u0440u043eu043au0443 u043fu043eu0434u043au043bu044eu0447u0435u043du0438u044f u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445
    if len(sys.argv) > 1:
        os.environ['DATABASE_URL'] = sys.argv[1]
    
    # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u043au043eu043cu0430u043du0434u044b /stats u0438 /checkusers
    fix_stats_and_checkusers()

if __name__ == "__main__":
    main()
