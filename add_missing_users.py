#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import psycopg2
import sqlite3
import datetime
import pytz
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

def add_missing_users():
    """
    u0414u043eu0431u0430u0432u043bu044fu0435u0442 u043eu0442u0441u0443u0442u0441u0442u0432u0443u044eu0449u0438u0445 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439 u0432 u0431u0430u0437u0443 u0434u0430u043du043du044bu0445
    """
    print("\n===== u0414u041eu0411u0410u0412u041bu0415u041du0418u0415 u041eu0422u0421u0423u0422u0421u0422u0412u0423u042eu0429u0418u0425 u041fu041eu041bu042cu0417u041eu0412u0410u0422u0415u041bu0415u0419 =====\n")
    
    # u0421u043fu0438u0441u043eu043a u043eu0442u0441u0443u0442u0441u0442u0432u0443u044eu0449u0438u0445 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439
    missing_users = [
        'Sebastianbachh',
        'JChvanova',
        'Nikita_Fateev',
        'TikhanovaStory'
    ]
    
    # u041fu043eu0434u043au043bu044eu0447u0430u0435u043cu0441u044f u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445
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
            print("u0422u0430u0431u043bu0438u0446u0430 'users' u043du0435 u0441u0443u0449u0435u0441u0442u0432u0443u0435u0442! u0421u043eu0437u0434u0430u0435u043c...")
            
            # u0421u043eu0437u0434u0430u0435u043c u0442u0430u0431u043bu0438u0446u0443 users
            if db_type == 'postgres':
                cursor.execute("""
                CREATE TABLE users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    registration_date TIMESTAMP,
                    is_admin BOOLEAN DEFAULT FALSE
                )
                """)
            else:
                cursor.execute("""
                CREATE TABLE users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    registration_date TIMESTAMP,
                    is_admin INTEGER DEFAULT 0
                )
                """)
                
            print("u0422u0430u0431u043bu0438u0446u0430 'users' u0443u0441u043fu0435u0448u043du043e u0441u043eu0437u0434u0430u043du0430!")
        
        # u0414u043eu0431u0430u0432u043bu044fu0435u043c u043eu0442u0441u0443u0442u0441u0442u0432u0443u044eu0449u0438u0445 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439
        now = datetime.datetime.now(pytz.timezone('Europe/Moscow'))
        
        for username in missing_users:
            # u0421u043eu0437u0434u0430u0435u043c u0443u043du0438u043au0430u043bu044cu043du044bu0439 ID u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044f (u043eu0442u0440u0438u0446u0430u0442u0435u043bu044cu043du044bu0439, u043au0430u043a u0443 u0434u0440u0443u0433u0438u0445 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439)
            temp_user_id = -1748032800000000 - random.randint(1, 999999)
            
            try:
                # u041fu0440u043eu0432u0435u0440u044fu0435u043c, u0435u0441u0442u044c u043bu0438 u0443u0436u0435 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044c u0441 u0442u0430u043au0438u043c username
                cursor.execute("SELECT user_id FROM users WHERE username = %s", (username,))
                existing_user = cursor.fetchone()
                
                if existing_user:
                    print(f"u041fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044c {username} u0443u0436u0435 u0441u0443u0449u0435u0441u0442u0432u0443u0435u0442 u0432 u0431u0430u0437u0435 u0434u0430u043du043du044bu0445 (ID: {existing_user[0]})")
                    continue
                
                # u0414u043eu0431u0430u0432u043bu044fu0435u043c u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044f
                cursor.execute("INSERT INTO users (user_id, username, registration_date, is_admin) VALUES (%s, %s, %s, %s)", 
                              (temp_user_id, username, now, False))
                
                print(f"u041fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044c {username} u0443u0441u043fu0435u0448u043du043e u0434u043eu0431u0430u0432u043bu0435u043d u0432 u0431u0430u0437u0443 u0434u0430u043du043du044bu0445 (ID: {temp_user_id})")
                
            except Exception as e:
                print(f"u041eu0448u0438u0431u043au0430 u043fu0440u0438 u0434u043eu0431u0430u0432u043bu0435u043du0438u0438 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044f {username}: {e}")
                conn.rollback()  # u041eu0442u043au0430u0442u044bu0432u0430u0435u043c u0442u0440u0430u043du0437u0430u043au0446u0438u044e u0434u043bu044f u044du0442u043eu0433u043e u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044f
        
        # u0421u043eu0445u0440u0430u043du044fu0435u043c u0438u0437u043cu0435u043du0435u043du0438u044f
        conn.commit()
        print("\nu0418u0437u043cu0435u043du0435u043du0438u044f u0443u0441u043fu0435u0448u043du043e u0441u043eu0445u0440u0430u043du0435u043du044b.")
        
        # u041fu0440u043eu0432u0435u0440u044fu0435u043c u0434u043eu0431u0430u0432u043bu0435u043du043du044bu0445 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439
        print("\nu041fu0440u043eu0432u0435u0440u043au0430 u0434u043eu0431u0430u0432u043bu0435u043du043du044bu0445 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439:")
        for username in missing_users:
            cursor.execute("SELECT user_id FROM users WHERE username = %s", (username,))
            result = cursor.fetchone()
            
            if result:
                print(f"  - {username}: u041du0410u0419u0414u0415u041d (ID: {result[0]})")
            else:
                print(f"  - {username}: u041du0415 u041du0410u0419u0414u0415u041d")
        
    except Exception as e:
        print(f"\nu041eu0448u0438u0431u043au0430 u043fu0440u0438 u0434u043eu0431u0430u0432u043bu0435u043du0438u0438 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439: {e}")
        conn.rollback()
    
    conn.close()
    print("\n===== u0414u041eu0411u0410u0412u041bu0415u041du0418u0415 u041eu0422u0421u0423u0422u0421u0422u0412u0423u042eu0429u0418u0425 u041fu041eu041bu042cu0417u041eu0412u0410u0422u0415u041bu0415u0419 u0417u0410u0412u0415u0420u0428u0415u041du041e =====\n")

def main():
    # u041fu043eu043bu0443u0447u0430u0435u043c u0441u0442u0440u043eu043au0443 u043fu043eu0434u043au043bu044eu0447u0435u043du0438u044f u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445
    if len(sys.argv) > 1:
        os.environ['DATABASE_URL'] = sys.argv[1]
    
    # u0414u043eu0431u0430u0432u043bu044fu0435u043c u043eu0442u0441u0443u0442u0441u0442u0432u0443u044eu0449u0438u0445 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439
    add_missing_users()

if __name__ == "__main__":
    main()
