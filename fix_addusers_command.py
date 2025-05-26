#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import psycopg2
import sqlite3
import re
from datetime import datetime
import pytz
import time
import random

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

def fix_add_users_command():
    """
    u0418u0441u043fu0440u0430u0432u043bu044fu0435u0442 u043au043eu043cu0430u043du0434u0443 /addusers, u0438u0437u043cu0435u043du044fu044f u0444u0443u043du043au0446u0438u044e add_users u0432 u0444u0430u0439u043bu0435 bot.py
    """
    print("\n===== u0418u0421u041fu0420u0410u0412u041bu0415u041du0418u0415 u041au041eu041cu0410u041du0414u042b /addusers =====\n")
    
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
        # u041fu0440u043eu0432u0435u0440u044fu0435u043c u0441u0443u0449u0435u0441u0442u0432u043eu0432u0430u043du0438u0435 u0442u0430u0431u043bu0438u0446u044b users
        if db_type == 'postgres':
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'users'
            );
            """)
        else:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
            
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print("u0422u0430u0431u043bu0438u0446u0430 'users' u043du0435 u0441u0443u0449u0435u0441u0442u0432u0443u0435u0442!")
            conn.close()
            return
        
        # u0422u0435u0441u0442u0438u0440u0443u0435u043c u043fu043eu0438u0441u043a u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439 u0441 u0440u0430u0437u043du044bu043cu0438 u0432u0430u0440u0438u0430u043du0442u0430u043cu0438 u0437u0430u043fu0440u043eu0441u043eu0432
        test_usernames = ['Sebastianbachh', 'sebastianbachh', 'SEBASTIANBACHH', '@Sebastianbachh']
        
        print("\nu0422u0435u0441u0442u0438u0440u043eu0432u0430u043du0438u0435 u043fu043eu0438u0441u043au0430 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439:")
        for username in test_usernames:
            clean_username = username[1:] if username.startswith('@') else username
            
            # u0422u0435u0441u0442 1: u0422u043eu0447u043du043eu0435 u0441u043eu0432u043fu0430u0434u0435u043du0438u0435
            if db_type == 'postgres':
                cursor.execute("SELECT user_id FROM users WHERE username = %s", (clean_username,))
            else:
                cursor.execute("SELECT user_id FROM users WHERE username = ?", (clean_username,))
                
            result1 = cursor.fetchone()
            
            # u0422u0435u0441u0442 2: u0421u043eu0432u043fu0430u0434u0435u043du0438u0435 u0431u0435u0437 u0443u0447u0435u0442u0430 u0440u0435u0433u0438u0441u0442u0440u0430
            if db_type == 'postgres':
                cursor.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(%s)", (clean_username,))
            else:
                cursor.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(?)", (clean_username,))
                
            result2 = cursor.fetchone()
            
            # u0422u0435u0441u0442 3: u0421u043eu0432u043fu0430u0434u0435u043du0438u0435 u0441 LIKE
            if db_type == 'postgres':
                cursor.execute("SELECT user_id FROM users WHERE username ILIKE %s", (clean_username,))
            else:
                cursor.execute("SELECT user_id FROM users WHERE username LIKE ? COLLATE NOCASE", (clean_username,))
                
            result3 = cursor.fetchone()
            
            print(f"  - {username}:")
            print(f"    * u0422u043eu0447u043du043eu0435 u0441u043eu0432u043fu0430u0434u0435u043du0438u0435: {result1[0] if result1 else 'u041du0415 u041du0410u0419u0414u0415u041d'}")
            print(f"    * LOWER(): {result2[0] if result2 else 'u041du0415 u041du0410u0419u0414u0415u041d'}")
            print(f"    * ILIKE/LIKE COLLATE NOCASE: {result3[0] if result3 else 'u041du0415 u041du0410u0419u0414u0415u041d'}")
        
        # u0422u0435u0441u0442u0438u0440u0443u0435u043c u0434u043eu0431u0430u0432u043bu0435u043du0438u0435 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044f u0441 u0440u0430u0437u043du044bu043cu0438 u0432u0430u0440u0438u0430u043du0442u0430u043cu0438 u0438u043cu0435u043du0438
        print("\nu0422u0435u0441u0442u0438u0440u043eu0432u0430u043du0438u0435 u0434u043eu0431u0430u0432u043bu0435u043du0438u044f u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044f:")
        test_username = "TestUser_" + str(int(time.time()))
        temp_user_id = -int(time.time()) - random.randint(1, 1000)
        now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"  - u0414u043eu0431u0430u0432u043bu044fu0435u043c u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044f {test_username} u0441 ID {temp_user_id}")
        
        try:
            if db_type == 'postgres':
                cursor.execute(
                    "INSERT INTO users (user_id, username, registration_date, is_admin) VALUES (%s, %s, %s, %s)", 
                    (temp_user_id, test_username, now, False)
                )
            else:
                cursor.execute(
                    "INSERT INTO users (user_id, username, registration_date, is_admin) VALUES (?, ?, ?, ?)", 
                    (temp_user_id, test_username, now, 0)
                )
                
            conn.commit()
            print(f"  - u041fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044c {test_username} u0443u0441u043fu0435u0448u043du043e u0434u043eu0431u0430u0432u043bu0435u043d")
            
            # u041fu0440u043eu0432u0435u0440u044fu0435u043c u043fu043eu0438u0441u043a u0441 u0440u0430u0437u043du044bu043cu0438 u0432u0430u0440u0438u0430u043du0442u0430u043cu0438 u0440u0435u0433u0438u0441u0442u0440u0430
            test_variants = [test_username, test_username.lower(), test_username.upper()]
            
            for variant in test_variants:
                if db_type == 'postgres':
                    cursor.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(%s)", (variant,))
                else:
                    cursor.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(?)", (variant,))
                    
                result = cursor.fetchone()
                print(f"  - u041fu043eu0438u0441u043a u043fu043e {variant}: {result[0] if result else 'u041du0415 u041du0410u0419u0414u0415u041d'}")
            
            # u0423u0434u0430u043bu044fu0435u043c u0442u0435u0441u0442u043eu0432u043eu0433u043e u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044f
            if db_type == 'postgres':
                cursor.execute("DELETE FROM users WHERE user_id = %s", (temp_user_id,))
            else:
                cursor.execute("DELETE FROM users WHERE user_id = ?", (temp_user_id,))
                
            conn.commit()
            print(f"  - u0422u0435u0441u0442u043eu0432u044bu0439 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044c u0443u0434u0430u043bu0435u043d")
            
        except Exception as e:
            print(f"  - u041eu0448u0438u0431u043au0430 u043fu0440u0438 u0442u0435u0441u0442u0438u0440u043eu0432u0430u043du0438u0438 u0434u043eu0431u0430u0432u043bu0435u043du0438u044f u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044f: {e}")
            conn.rollback()
        
        # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0444u0443u043du043au0446u0438u044e add_users u0432 bot.py
        print("\nu0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0444u0443u043du043au0446u0438u044e add_users u0432 bot.py...")
        
        # u0421u043eu0437u0434u0430u0435u043c u0440u0435u0437u0435u0440u0432u043du0443u044e u043au043eu043fu0438u044e bot.py
        backup_path = bot_py_path + '.bak'
        with open(bot_py_path, 'r', encoding='utf-8') as f_in, open(backup_path, 'w', encoding='utf-8') as f_out:
            f_out.write(f_in.read())
        
        print(f"  - u0421u043eu0437u0434u0430u043du0430 u0440u0435u0437u0435u0440u0432u043du0430u044f u043au043eu043fu0438u044f {backup_path}")
        
        # u0427u0438u0442u0430u0435u043c u0444u0430u0439u043b bot.py
        with open(bot_py_path, 'r', encoding='utf-8') as f:
            bot_py_content = f.read()
        
        # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0437u0430u043fu0440u043eu0441 u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445 u0432 u0444u0443u043du043au0446u0438u0438 add_users
        # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0441u0442u0440u043eu043au0443 u0441 u043fu0440u043eu0432u0435u0440u043au043eu0439 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044f u0432 u0442u0430u0431u043bu0438u0446u0435 users
        old_code_1 = "cursor.execute(\"SELECT user_id FROM users WHERE LOWER(username) = LOWER(%s)\", (username,))"
        new_code_1 = "cursor.execute(\"SELECT user_id FROM users WHERE LOWER(username) = LOWER(%s) OR LOWER(username) = LOWER('@' || %s)\", (username, username))"
        
        old_code_2 = "cursor.execute(\"SELECT user_id FROM users WHERE LOWER(username) = LOWER(?)\", (username,))"
        new_code_2 = "cursor.execute(\"SELECT user_id FROM users WHERE LOWER(username) = LOWER(?) OR LOWER(username) = LOWER('@' || ?)\", (username, username))"
        
        # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0441u0442u0440u043eu043au0443 u0441 u043fu0440u043eu0432u0435u0440u043au043eu0439 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044f u0432 u0442u0430u0431u043bu0438u0446u0435 pending_users
        old_code_3 = "cursor.execute(\"SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(%s)\", (username,))"
        new_code_3 = "cursor.execute(\"SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(%s) OR LOWER(username) = LOWER('@' || %s)\", (username, username))"
        
        old_code_4 = "cursor.execute(\"SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(?)\", (username,))"
        new_code_4 = "cursor.execute(\"SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(?) OR LOWER(username) = LOWER('@' || ?)\", (username, username))"
        
        # u0417u0430u043cu0435u043du044fu0435u043c u043au043eu0434
        bot_py_content = bot_py_content.replace(old_code_1, new_code_1)
        bot_py_content = bot_py_content.replace(old_code_2, new_code_2)
        bot_py_content = bot_py_content.replace(old_code_3, new_code_3)
        bot_py_content = bot_py_content.replace(old_code_4, new_code_4)
        
        # u0417u0430u043fu0438u0441u044bu0432u0430u0435u043c u0438u0437u043cu0435u043du0435u043du0438u044f u0432 u0444u0430u0439u043b
        with open(bot_py_path, 'w', encoding='utf-8') as f:
            f.write(bot_py_content)
        
        print("  - u0424u0443u043du043au0446u0438u044f add_users u0443u0441u043fu0435u0448u043du043e u0438u0441u043fu0440u0430u0432u043bu0435u043du0430")
        print("  - u0422u0435u043fu0435u0440u044c u043au043eu043cu0430u043du0434u0430 /addusers u0431u0443u0434u0435u0442 u0438u0441u043au0430u0442u044c u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439 u0441 u0443u0447u0435u0442u043eu043c u0440u0430u0437u043du044bu0445 u0432u0430u0440u0438u0430u043du0442u043eu0432 u043du0430u043fu0438u0441u0430u043du0438u044f u0438u043cu0435u043du0438")
        print("  - u0422u0430u043au0436u0435 u043au043eu043cu0430u043du0434u0430 u0431u0443u0434u0435u0442 u0438u0441u043au0430u0442u044c u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439 u043au0430u043a u0441 @, u0442u0430u043a u0438 u0431u0435u0437 @")
        
    except Exception as e:
        print(f"\nu041eu0448u0438u0431u043au0430 u043fu0440u0438 u0438u0441u043fu0440u0430u0432u043bu0435u043du0438u0438 u043au043eu043cu0430u043du0434u044b /addusers: {e}")
    
    conn.close()
    print("\n===== u0418u0421u041fu0420u0410u0412u041bu0415u041du0418u0415 u041au041eu041cu0410u041du0414u042b /addusers u0417u0410u0412u0415u0420u0428u0415u041du041e =====\n")

def main():
    # u041fu043eu043bu0443u0447u0430u0435u043c u0441u0442u0440u043eu043au0443 u043fu043eu0434u043au043bu044eu0447u0435u043du0438u044f u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445
    if len(sys.argv) > 1:
        os.environ['DATABASE_URL'] = sys.argv[1]
    
    # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u043au043eu043cu0430u043du0434u0443 /addusers
    fix_add_users_command()

if __name__ == "__main__":
    main()
