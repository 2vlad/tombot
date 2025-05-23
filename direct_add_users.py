#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import psycopg2
from datetime import datetime
import pytz
import time
import random

# u0421u043fu0438u0441u043eu043a u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439 u0434u043bu044f u0434u043eu0431u0430u0432u043bu0435u043du0438u044f
USERS_TO_ADD = [
    'nastyaglukhikh',
    'sebastianbach',
    'valerigeb',
    'jchvanova',
    'mabublik',
    'nikitafateev',
    'oxanatimchenko'
]

def direct_add_users_to_railway(database_url, users):
    """
    u041du0430u043fu0440u044fu043cu0443u044e u0434u043eu0431u0430u0432u043bu044fu0435u0442 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439 u0432 u0431u0430u0437u0443 u0434u0430u043du043du044bu0445 PostgreSQL u043du0430 Railway
    
    Args:
        database_url (str): u0421u0442u0440u043eu043au0430 u043fu043eu0434u043au043bu044eu0447u0435u043du0438u044f u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445 PostgreSQL
        users (list): u0421u043fu0438u0441u043eu043a u0438u043cu0435u043d u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439 u0434u043bu044f u0434u043eu0431u0430u0432u043bu0435u043du0438u044f
    """
    print(f"\nu041fu043eu0434u043au043bu044eu0447u0430u044eu0441u044c u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445 PostgreSQL u043du0430 Railway...")
    
    try:
        # u041fu043eu0434u043au043bu044eu0447u0430u0435u043cu0441u044f u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        print("u0423u0441u043fu0435u0448u043du043e u043fu043eu0434u043au043bu044eu0447u0438u043bu0438u0441u044c u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445 PostgreSQL!")
        
        # u041fu0440u043eu0432u0435u0440u044fu0435u043c u043du0430u043bu0438u0447u0438u0435 u0442u0430u0431u043bu0438u0446u044b users
        cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'users'
        );
        """)
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print("u0422u0430u0431u043bu0438u0446u0430 'users' u043du0435 u0441u0443u0449u0435u0441u0442u0432u0443u0435u0442. u0421u043eu0437u0434u0430u044e...")
            
            # u0421u043eu0437u0434u0430u0435u043c u0442u0430u0431u043bu0438u0446u0443 users
            cursor.execute("""
            CREATE TABLE users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                registration_date TIMESTAMP,
                is_admin BOOLEAN DEFAULT FALSE,
                first_name TEXT,
                last_name TEXT,
                phone_number TEXT
            )
            """)
            
            print("u0422u0430u0431u043bu0438u0446u0430 'users' u0443u0441u043fu0435u0448u043du043e u0441u043eu0437u0434u0430u043du0430!")
        
        # u0414u043eu0431u0430u0432u043bu044fu0435u043c u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439
        added_count = 0
        already_exists_count = 0
        
        print(f"\nu041du0430u0447u0438u043du0430u044e u0434u043eu0431u0430u0432u043bu0435u043du0438u0435 {len(users)} u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439...")
        
        for username in users:
            username = username.lower()
            
            # u041fu0440u043eu0432u0435u0440u044fu0435u043c, u0435u0441u0442u044c u043bu0438 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044c u0443u0436u0435 u0432 u0442u0430u0431u043bu0438u0446u0435 users
            cursor.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(%s)", (username,))
            existing_user = cursor.fetchone()
            
            if existing_user:
                print(f"u041fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044c @{username} u0443u0436u0435 u0441u0443u0449u0435u0441u0442u0432u0443u0435u0442 u0441 ID {existing_user[0]}")
                already_exists_count += 1
                continue
            
            # u0421u043eu0437u0434u0430u0435u043c u0432u0440u0435u043cu0435u043du043du044bu0439 ID (u043eu0442u0440u0438u0446u0430u0442u0435u043bu044cu043du043eu0435 u0447u0438u0441u043bu043e)
            temp_user_id = -int(time.time()) - random.randint(1, 1000)
            now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')
            
            # u0414u043eu0431u0430u0432u043bu044fu0435u043c u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044f u0432 u0442u0430u0431u043bu0438u0446u0443 users
            try:
                cursor.execute(
                    "INSERT INTO users (user_id, username, registration_date, is_admin) VALUES (%s, %s, %s, %s)", 
                    (temp_user_id, username, now, False)
                )
                
                print(f"u0414u043eu0431u0430u0432u043bu0435u043d u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044c @{username} u0441 ID {temp_user_id}")
                added_count += 1
                
            except Exception as e:
                print(f"u041eu0448u0438u0431u043au0430 u043fu0440u0438 u0434u043eu0431u0430u0432u043bu0435u043du0438u0438 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu044f @{username}: {e}")
        
        # u0421u043eu0445u0440u0430u043du044fu0435u043c u0438u0437u043cu0435u043du0435u043du0438u044f
        conn.commit()
        
        # u041fu0440u043eu0432u0435u0440u044fu0435u043c u0440u0435u0437u0443u043bu044cu0442u0430u0442
        print(f"\nu0420u0435u0437u0443u043bu044cu0442u0430u0442u044b:")
        print(f"u0414u043eu0431u0430u0432u043bu0435u043du043e u043du043eu0432u044bu0445 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439: {added_count}")
        print(f"u0423u0436u0435 u0441u0443u0449u0435u0441u0442u0432u043eu0432u0430u043bu0438: {already_exists_count}")
        
        # u041fu0440u043eu0432u0435u0440u044fu0435u043c, u0447u0442u043e u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0438 u0434u0435u0439u0441u0442u0432u0438u0442u0435u043bu044cu043du043e u0434u043eu0431u0430u0432u043bu0435u043du044b
        print("\nu041fu0440u043eu0432u0435u0440u044fu0435u043c u0434u043eu0431u0430u0432u043bu0435u043du043du044bu0445 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439:")
        for username in users:
            username = username.lower()
            cursor.execute("SELECT user_id, username FROM users WHERE LOWER(username) = LOWER(%s)", (username,))
            user = cursor.fetchone()
            if user:
                print(f"u2713 @{username} u043du0430u0439u0434u0435u043d u0441 ID {user[0]}")
            else:
                print(f"u2717 @{username} u041du0415 u043du0430u0439u0434u0435u043d")
        
        conn.close()
        print("\nu0414u043eu0431u0430u0432u043bu0435u043du0438u0435 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439 u0437u0430u0432u0435u0440u0448u0435u043du043e!")
        
    except Exception as e:
        print(f"\nu041eu0448u0438u0431u043au0430 u043fu0440u0438 u0440u0430u0431u043eu0442u0435 u0441 u0431u0430u0437u043eu0439 u0434u0430u043du043du044bu0445: {e}")

def main():
    # u041fu043eu043bu0443u0447u0430u0435u043c u0441u0442u0440u043eu043au0443 u043fu043eu0434u043au043bu044eu0447u0435u043du0438u044f u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445
    if len(sys.argv) > 1:
        database_url = sys.argv[1]
    else:
        database_url = input("u0412u0432u0435u0434u0438u0442u0435 u0441u0442u0440u043eu043au0443 u043fu043eu0434u043au043bu044eu0447u0435u043du0438u044f u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445 PostgreSQL u043du0430 Railway: ")
    
    # u041fu043eu043bu0443u0447u0430u0435u043c u0441u043fu0438u0441u043eu043a u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439
    users = USERS_TO_ADD
    
    # u0414u043eu0431u0430u0432u043bu044fu0435u043c u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439
    direct_add_users_to_railway(database_url, users)

if __name__ == "__main__":
    main()
