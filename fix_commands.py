#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re

def fix_commands():
    """
    u0418u0441u043fu0440u0430u0432u043bu044fu0435u0442 u043au043eu043cu0430u043du0434u044b /stats u0438 /checkusers u0432 u0444u0430u0439u043bu0435 bot.py
    """
    print("\n===== u0418u0421u041fu0420u0410u0412u041bu0415u041du0418u0415 u041au041eu041cu0410u041du0414 /stats u0418 /checkusers =====\n")
    
    # u041fu0443u0442u044c u043a u0444u0430u0439u043bu0443 bot.py
    bot_py_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bot.py')
    
    # u041fu0440u043eu0432u0435u0440u044fu0435u043c, u0441u0443u0449u0435u0441u0442u0432u0443u0435u0442 u043bu0438 u0444u0430u0439u043b
    if not os.path.exists(bot_py_path):
        print(f"\nu041eu0448u0438u0431u043au0430: u0444u0430u0439u043b {bot_py_path} u043du0435 u043du0430u0439u0434u0435u043d")
        return
    
    try:
        # u0421u043eu0437u0434u0430u0435u043c u0440u0435u0437u0435u0440u0432u043du0443u044e u043au043eu043fu0438u044e bot.py
        backup_path = bot_py_path + '.commands.bak'
        with open(bot_py_path, 'r', encoding='utf-8') as f_in, open(backup_path, 'w', encoding='utf-8') as f_out:
            f_out.write(f_in.read())
        
        print(f"  - u0421u043eu0437u0434u0430u043du0430 u0440u0435u0437u0435u0440u0432u043du0430u044f u043au043eu043fu0438u044f {backup_path}")
        
        # u0427u0438u0442u0430u0435u043c u0444u0430u0439u043b bot.py
        with open(bot_py_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0444u0443u043du043au0446u0438u044e show_stats
        print("1. u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0444u0443u043du043au0446u0438u044e show_stats...")
        
        # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u043fu043eu0434u043au043bu044eu0447u0435u043du0438u0435 u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445 u0432 u0444u0443u043du043au0446u0438u0438 show_stats
        for i in range(len(lines)):
            # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u043fu043eu0434u043au043bu044eu0447u0435u043du0438u0435 u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445 u0432 show_stats
            if "def show_stats" in lines[i] and i+10 < len(lines):
                for j in range(i, i+10):
                    if "conn = sqlite3.connect('filmschool.db')" in lines[j]:
                        lines[j] = "    conn, db_type = get_db_connection()\n"
                        print("  - u0418u0441u043fu0440u0430u0432u043bu0435u043du043e u043fu043eu0434u043au043bu044eu0447u0435u043du0438u0435 u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445 u0432 show_stats")
                        break
            
            # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0437u0430u043fu0440u043eu0441 u043du0430 u043fu043eu0434u0441u0447u0435u0442 u0430u0434u043cu0438u043du0438u0441u0442u0440u0430u0442u043eu0440u043eu0432
            if "cursor.execute(\"SELECT COUNT(*) FROM users WHERE is_admin = 1\")" in lines[i]:
                lines[i] = "    if db_type == 'postgres':\n        cursor.execute(\"SELECT COUNT(*) FROM users WHERE is_admin = TRUE\")\n    else:\n        cursor.execute(\"SELECT COUNT(*) FROM users WHERE is_admin = 1\")\n"
                print("  - u0418u0441u043fu0440u0430u0432u043bu0435u043d u0437u0430u043fu0440u043eu0441 u043du0430 u043fu043eu0434u0441u0447u0435u0442 u0430u0434u043cu0438u043du0438u0441u0442u0440u0430u0442u043eu0440u043eu0432")
            
            # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0437u0430u043fu0440u043eu0441u044b u0441 GROUP_CONCAT u0434u043bu044f PostgreSQL
            if "GROUP_CONCAT(timestamp, ', ')" in lines[i] and "get_latest_video" in lines[i+1]:
                # u041du0430u0445u043eu0434u0438u043c u043du0430u0447u0430u043bu043e u0437u0430u043fu0440u043eu0441u0430
                start_idx = i - 1
                while "cursor.execute(" not in lines[start_idx]:
                    start_idx -= 1
                
                # u041du0430u0445u043eu0434u0438u043c u043au043eu043du0435u0446 u0437u0430u043fu0440u043eu0441u0430
                end_idx = i + 5
                while "\"\")" not in lines[end_idx]:
                    end_idx += 1
                
                # u0417u0430u043cu0435u043du044fu0435u043c u0437u0430u043fu0440u043eu0441 u043du0430 u0432u0435u0440u0441u0438u044e u0441 u043fu043eu0434u0434u0435u0440u0436u043au043eu0439 PostgreSQL
                pg_query = "    if db_type == 'postgres':\n"
                pg_query += "        cursor.execute(\"\"\"\
"
                pg_query += "        SELECT username, first_name, last_name, user_id, STRING_AGG(timestamp::text, ', ') as access_times\n"
                pg_query += "        FROM logs \n"
                pg_query += "        WHERE action = 'get_latest_video'\n"
                pg_query += "        GROUP BY COALESCE(username, user_id), username, first_name, last_name, user_id\n"
                pg_query += "        ORDER BY MAX(timestamp) DESC\n"
                pg_query += "        \"\"\")\n"
                pg_query += "    else:\n"
                pg_query += "        cursor.execute(\"\"\"\
"
                pg_query += "        SELECT username, first_name, last_name, user_id, GROUP_CONCAT(timestamp, ', ') as access_times\n"
                pg_query += "        FROM logs \n"
                pg_query += "        WHERE action = 'get_latest_video'\n"
                pg_query += "        GROUP BY COALESCE(username, user_id)\n"
                pg_query += "        ORDER BY MAX(timestamp) DESC\n"
                pg_query += "        \"\"\")\n"
                
                # u0417u0430u043cu0435u043du044fu0435u043c u0441u0442u0430u0440u044bu0439 u0437u0430u043fu0440u043eu0441 u043du0430 u043du043eu0432u044bu0439
                lines[start_idx:end_idx+1] = [pg_query]
                print("  - u0418u0441u043fu0440u0430u0432u043bu0435u043d u0437u0430u043fu0440u043eu0441 u0441 GROUP_CONCAT u0434u043bu044f get_latest_video")
            
            # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0437u0430u043fu0440u043eu0441u044b u0441 GROUP_CONCAT u0434u043bu044f PostgreSQL (u0434u043bu044f previous_video)
            if "GROUP_CONCAT(timestamp, ', ')" in lines[i] and "get_previous_video" in lines[i+1]:
                # u041du0430u0445u043eu0434u0438u043c u043du0430u0447u0430u043bu043e u0437u0430u043fu0440u043eu0441u0430
                start_idx = i - 1
                while "cursor.execute(" not in lines[start_idx]:
                    start_idx -= 1
                
                # u041du0430u0445u043eu0434u0438u043c u043au043eu043du0435u0446 u0437u0430u043fu0440u043eu0441u0430
                end_idx = i + 5
                while "\"\")" not in lines[end_idx]:
                    end_idx += 1
                
                # u0417u0430u043cu0435u043du044fu0435u043c u0437u0430u043fu0440u043eu0441 u043du0430 u0432u0435u0440u0441u0438u044e u0441 u043fu043eu0434u0434u0435u0440u0436u043au043eu0439 PostgreSQL
                pg_query = "    if db_type == 'postgres':\n"
                pg_query += "        cursor.execute(\"\"\"\
"
                pg_query += "        SELECT username, first_name, last_name, user_id, STRING_AGG(timestamp::text, ', ') as access_times\n"
                pg_query += "        FROM logs \n"
                pg_query += "        WHERE action = 'get_previous_video'\n"
                pg_query += "        GROUP BY COALESCE(username, user_id), username, first_name, last_name, user_id\n"
                pg_query += "        ORDER BY MAX(timestamp) DESC\n"
                pg_query += "        \"\"\")\n"
                pg_query += "    else:\n"
                pg_query += "        cursor.execute(\"\"\"\
"
                pg_query += "        SELECT username, first_name, last_name, user_id, GROUP_CONCAT(timestamp, ', ') as access_times\n"
                pg_query += "        FROM logs \n"
                pg_query += "        WHERE action = 'get_previous_video'\n"
                pg_query += "        GROUP BY COALESCE(username, user_id)\n"
                pg_query += "        ORDER BY MAX(timestamp) DESC\n"
                pg_query += "        \"\"\")\n"
                
                # u0417u0430u043cu0435u043du044fu0435u043c u0441u0442u0430u0440u044bu0439 u0437u0430u043fu0440u043eu0441 u043du0430 u043du043eu0432u044bu0439
                lines[start_idx:end_idx+1] = [pg_query]
                print("  - u0418u0441u043fu0440u0430u0432u043bu0435u043d u0437u0430u043fu0440u043eu0441 u0441 GROUP_CONCAT u0434u043bu044f get_previous_video")
        
        # 2. u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0444u0443u043du043au0446u0438u044e check_users
        print("\n2. u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0444u0443u043du043au0446u0438u044e check_users...")
        
        for i in range(len(lines)):
            # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0437u0430u043fu0440u043eu0441 u043a u0442u0430u0431u043bu0438u0446u0435 pending_users
            if "cursor.execute(\"SELECT user_id FROM pending_users WHERE username = %s\"" in lines[i]:
                lines[i] = "            cursor.execute(\"SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(%s) OR LOWER(username) = LOWER('@' || %s)\", (username, username))\n"
                print("  - u0418u0441u043fu0440u0430u0432u043bu0435u043d u0437u0430u043fu0440u043eu0441 u043a u0442u0430u0431u043bu0438u0446u0435 pending_users u0432 PostgreSQL")
            
            if "cursor.execute(\"SELECT user_id FROM pending_users WHERE username = ?\"" in lines[i]:
                lines[i] = "            cursor.execute(\"SELECT user_id FROM pending_users WHERE LOWER(username) = LOWER(?) OR LOWER(username) = LOWER('@' || ?)\", (username, username))\n"
                print("  - u0418u0441u043fu0440u0430u0432u043bu0435u043d u0437u0430u043fu0440u043eu0441 u043a u0442u0430u0431u043bu0438u0446u0435 pending_users u0432 SQLite")
        
        # u0417u0430u043fu0438u0441u044bu0432u0430u0435u043c u0438u0437u043cu0435u043du0435u043du0438u044f u0432 u0444u0430u0439u043b
        with open(bot_py_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        print("\nu0424u0443u043du043au0446u0438u0438 show_stats u0438 check_users u0443u0441u043fu0435u0448u043du043e u0438u0441u043fu0440u0430u0432u043bu0435u043du044b")
        print("u0422u0435u043fu0435u0440u044c u043au043eu043cu0430u043du0434u044b /stats u0438 /checkusers u0431u0443u0434u0443u0442 u0440u0430u0431u043eu0442u0430u0442u044c u043au043eu0440u0440u0435u043au0442u043du043e u0441 PostgreSQL")
        
    except Exception as e:
        print(f"\nu041eu0448u0438u0431u043au0430 u043fu0440u0438 u0438u0441u043fu0440u0430u0432u043bu0435u043du0438u0438 u043au043eu043cu0430u043du0434 /stats u0438 /checkusers: {e}")
    
    print("\n===== u0418u0421u041fu0420u0410u0412u041bu0415u041du0418u0415 u041au041eu041cu0410u041du0414 /stats u0418 /checkusers u0417u0410u0412u0415u0420u0428u0415u041du041e =====\n")

def main():
    fix_commands()

if __name__ == "__main__":
    main()
