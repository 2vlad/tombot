#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import logging

# u041du0430u0441u0442u0440u043eu0439u043au0430 u043bu043eu0433u0433u0438u0440u043eu0432u0430u043du0438u044f
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def patch_load_buttons():
    """
    u041fu0430u0442u0447u0438u0442 u0444u0443u043du043au0446u0438u044e load_buttons u0432 u0444u0430u0439u043bu0435 db_utils.py
    """
    print("u041du0430u0447u0438u043du0430u044e u043fu0430u0442u0447 u0444u0443u043du043au0446u0438u0438 load_buttons...")
    
    # u041fu0443u0442u044c u043a u0444u0430u0439u043bu0443 db_utils.py
    db_utils_path = 'db_utils.py'
    
    # u041fu0440u043eu0432u0435u0440u044fu0435u043c, u0447u0442u043e u0444u0430u0439u043b u0441u0443u0449u0435u0441u0442u0432u0443u0435u0442
    if not os.path.exists(db_utils_path):
        print(f"u0424u0430u0439u043b {db_utils_path} u043du0435 u043du0430u0439u0434u0435u043d")
        return
    
    # u0427u0438u0442u0430u0435u043c u0441u043eu0434u0435u0440u0436u0438u043cu043eu0435 u0444u0430u0439u043bu0430
    with open(db_utils_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # u041du0430u0445u043eu0434u0438u043c u0444u0443u043du043au0446u0438u044e load_buttons
    load_buttons_pattern = r'def load_buttons\(\):(?:[^\n]*\n)+?(?:    return buttons\n)'
    load_buttons_match = re.search(load_buttons_pattern, content)
    
    if not load_buttons_match:
        print("u0424u0443u043du043au0446u0438u044f load_buttons u043du0435 u043du0430u0439u0434u0435u043du0430 u0432 u0444u0430u0439u043bu0435")
        return
    
    # u041du043eu0432u0430u044f u0440u0435u0430u043bu0438u0437u0430u0446u0438u044f u0444u0443u043du043au0446u0438u0438 load_buttons
    new_load_buttons = '''def load_buttons():
    """
    u0417u0430u0433u0440u0443u0436u0430u0435u0442 u043du0430u0441u0442u0440u043eu0439u043au0438 u043au043du043eu043fu043eu043a u0438u0437 u0431u0430u0437u044b u0434u0430u043du043du044bu0445.
    u0412u043eu0437u0432u0440u0430u0449u0430u0435u0442 u0441u043bu043eu0432u0430u0440u044c u0441 u043du0430u0441u0442u0440u043eu0439u043au0430u043cu0438 u043au043du043eu043fu043eu043a.
    """
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    buttons = {}
    
    # u041fu0440u043eu0432u0435u0440u044fu0435u043c, u0435u0441u0442u044c u043bu0438 u043au043du043eu043fu043au0438 u0432 u0431u0430u0437u0435 u0434u0430u043du043du044bu0445
    try:
        if db_type == DB_TYPE_POSTGRES:
            cursor.execute("SELECT COUNT(*) FROM buttons")
        else:
            cursor.execute("SELECT COUNT(*) FROM buttons")
        
        count = cursor.fetchone()[0]
        
        if count > 0:
            # u0417u0430u0433u0440u0443u0436u0430u0435u043c u043du0430u0441u0442u0440u043eu0439u043au0438 u043au043du043eu043fu043eu043a
            if db_type == DB_TYPE_POSTGRES:
                cursor.execute("SELECT button_key, button_text, button_url FROM buttons")
            else:
                cursor.execute("SELECT button_key, button_text, button_url FROM buttons")
            
            for row in cursor.fetchall():
                button_key, button_text, button_url = row
                # u041fu0440u0435u043eu0431u0440u0430u0437u0443u0435u043c button_key u0432 u043du043eu043cu0435u0440 u043au043du043eu043fu043au0438 (u043du0430u043fu0440u0438u043cu0435u0440, 'button1' -> 1)
                if button_key.startswith('button') and button_key[6:].isdigit():
                    button_number = int(button_key[6:])
                    buttons[button_number] = {'text': button_text, 'message': button_url}
    except Exception as e:
        logger.error(f"Error loading buttons: {e}")
    
    conn.close()
    return buttons
'''
    
    # u0417u0430u043cu0435u043du044fu0435u043c u0441u0442u0430u0440u0443u044e u0444u0443u043du043au0446u0438u044e u043du0430 u043du043eu0432u0443u044e
    new_content = content.replace(load_buttons_match.group(0), new_load_buttons)
    
    # u0421u043eu0445u0440u0430u043du044fu0435u043c u0438u0437u043cu0435u043du0435u043du0438u044f
    with open(db_utils_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print("u0424u0443u043du043au0446u0438u044f load_buttons u0443u0441u043fu0435u0448u043du043e u043fu0430u0442u0447u0435u043du0430!")

if __name__ == "__main__":
    patch_load_buttons()
