#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import psycopg2
import sqlite3

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

def fix_logs_table():
    """
    u0418u0441u043fu0440u0430u0432u043bu044fu0435u0442 u0442u0430u0431u043bu0438u0446u0443 logs, u0434u043eu0431u0430u0432u043bu044fu044f u043au043eu043bu043eu043du043au0438 first_name u0438 last_name, u0435u0441u043bu0438 u043eu043du0438 u043eu0442u0441u0443u0442u0441u0442u0432u0443u044eu0442
    """
    print("\n===== u0418u0421u041fu0420u0410u0412u041bu0415u041du0418u0415 u0422u0410u0411u041bu0418u0426u042b LOGS =====\n")
    
    # u041fu043eu0434u043au043bu044eu0447u0430u0435u043cu0441u044f u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # u041fu0440u043eu0432u0435u0440u044fu0435u043c u0441u0443u0449u0435u0441u0442u0432u043eu0432u0430u043du0438u0435 u0442u0430u0431u043bu0438u0446u044b logs
        if db_type == 'postgres':
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'logs'
            );
            """)
        else:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='logs'")
            
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print("u0422u0430u0431u043bu0438u0446u0430 'logs' u043du0435 u0441u0443u0449u0435u0441u0442u0432u0443u0435u0442. u0421u043eu0437u0434u0430u0435u043c...")
            
            # u0421u043eu0437u0434u0430u0435u043c u0442u0430u0431u043bu0438u0446u0443 logs u0441 u043au043eu043bu043eu043du043au0430u043cu0438 first_name u0438 last_name
            if db_type == 'postgres':
                cursor.execute("""
                CREATE TABLE logs (
                    log_id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    action_type TEXT,
                    action_data TEXT,
                    action_date TIMESTAMP DEFAULT NOW()
                )
                """)
            else:
                cursor.execute("""
                CREATE TABLE logs (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    action_type TEXT,
                    action_data TEXT,
                    action_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
            print("u0422u0430u0431u043bu0438u0446u0430 'logs' u0443u0441u043fu0435u0448u043du043e u0441u043eu0437u0434u0430u043du0430 u0441 u043au043eu043bu043eu043du043au0430u043cu0438 first_name u0438 last_name!")
        else:
            # u041fu0440u043eu0432u0435u0440u044fu0435u043c u043du0430u043bu0438u0447u0438u0435 u043au043eu043bu043eu043du043au0438 first_name
            if db_type == 'postgres':
                cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'logs' 
                    AND column_name = 'first_name'
                );
                """)
                first_name_exists = cursor.fetchone()[0]
                
                cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'logs' 
                    AND column_name = 'last_name'
                );
                """)
                last_name_exists = cursor.fetchone()[0]
            else:
                cursor.execute("PRAGMA table_info(logs)")
                columns = cursor.fetchall()
                first_name_exists = any(col[1] == 'first_name' for col in columns)
                last_name_exists = any(col[1] == 'last_name' for col in columns)
            
            # u0414u043eu0431u0430u0432u043bu044fu0435u043c u043au043eu043bu043eu043du043au0443 first_name, u0435u0441u043bu0438 u043eu043du0430 u043eu0442u0441u0443u0442u0441u0442u0432u0443u0435u0442
            if not first_name_exists:
                print("u041au043eu043bu043eu043du043au0430 'first_name' u043eu0442u0441u0443u0442u0441u0442u0432u0443u0435u0442. u0414u043eu0431u0430u0432u043bu044fu0435u043c...")
                
                if db_type == 'postgres':
                    cursor.execute("ALTER TABLE logs ADD COLUMN first_name TEXT")
                else:
                    cursor.execute("ALTER TABLE logs ADD COLUMN first_name TEXT")
                    
                print("u041au043eu043bu043eu043du043au0430 'first_name' u0443u0441u043fu0435u0448u043du043e u0434u043eu0431u0430u0432u043bu0435u043du0430!")
            else:
                print("u041au043eu043bu043eu043du043au0430 'first_name' u0443u0436u0435 u0441u0443u0449u0435u0441u0442u0432u0443u0435u0442.")
            
            # u0414u043eu0431u0430u0432u043bu044fu0435u043c u043au043eu043bu043eu043du043au0443 last_name, u0435u0441u043bu0438 u043eu043du0430 u043eu0442u0441u0443u0442u0441u0442u0432u0443u0435u0442
            if not last_name_exists:
                print("u041au043eu043bu043eu043du043au0430 'last_name' u043eu0442u0441u0443u0442u0441u0442u0432u0443u0435u0442. u0414u043eu0431u0430u0432u043bu044fu0435u043c...")
                
                if db_type == 'postgres':
                    cursor.execute("ALTER TABLE logs ADD COLUMN last_name TEXT")
                else:
                    cursor.execute("ALTER TABLE logs ADD COLUMN last_name TEXT")
                    
                print("u041au043eu043bu043eu043du043au0430 'last_name' u0443u0441u043fu0435u0448u043du043e u0434u043eu0431u0430u0432u043bu0435u043du0430!")
            else:
                print("u041au043eu043bu043eu043du043au0430 'last_name' u0443u0436u0435 u0441u0443u0449u0435u0441u0442u0432u0443u0435u0442.")
        
        # u0412u044bu0432u043eu0434u0438u043c u0442u0435u043au0443u0449u0443u044e u0441u0442u0440u0443u043au0442u0443u0440u0443 u0442u0430u0431u043bu0438u0446u044b logs
        if db_type == 'postgres':
            cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'logs'
            """)
            
            columns = cursor.fetchall()
            print("\nu0422u0435u043au0443u0449u0430u044f u0441u0442u0440u0443u043au0442u0443u0440u0430 u0442u0430u0431u043bu0438u0446u044b logs:")
            for col in columns:
                print(f"  - {col[0]}: {col[1]}")
        else:
            cursor.execute("PRAGMA table_info(logs)")
            columns = cursor.fetchall()
            print("\nu0422u0435u043au0443u0449u0430u044f u0441u0442u0440u0443u043au0442u0443u0440u0430 u0442u0430u0431u043bu0438u0446u044b logs:")
            for col in columns:
                print(f"  - {col[1]}: {col[2]}")
        
        # u0421u043eu0445u0440u0430u043du044fu0435u043c u0438u0437u043cu0435u043du0435u043du0438u044f
        conn.commit()
        print("\nu0418u0437u043cu0435u043du0435u043du0438u044f u0443u0441u043fu0435u0448u043du043e u0441u043eu0445u0440u0430u043du0435u043du044b.")
        
    except Exception as e:
        print(f"\nu041eu0448u0438u0431u043au0430 u043fu0440u0438 u0438u0441u043fu0440u0430u0432u043bu0435u043du0438u0438 u0442u0430u0431u043bu0438u0446u044b logs: {e}")
        conn.rollback()
    
    conn.close()
    print("\n===== u0418u0421u041fu0420u0410u0412u041bu0415u041du0418u0415 u0422u0410u0411u041bu0418u0426u042b LOGS u0417u0410u0412u0415u0420u0428u0415u041du041e =====\n")

def main():
    # u041fu043eu043bu0443u0447u0430u0435u043c u0441u0442u0440u043eu043au0443 u043fu043eu0434u043au043bu044eu0447u0435u043du0438u044f u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445
    if len(os.sys.argv) > 1:
        os.environ['DATABASE_URL'] = os.sys.argv[1]
    
    # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u0442u0430u0431u043bu0438u0446u0443 logs
    fix_logs_table()

if __name__ == "__main__":
    main()
