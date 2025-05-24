#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re

def fix_multiline_strings():
    """
    Fixes the multiline string literals in the show_stats function
    """
    print("\n===== FIXING MULTILINE STRINGS IN SHOW_STATS =====\n")
    
    # Path to bot.py file
    bot_py_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bot.py')
    
    # Check if file exists
    if not os.path.exists(bot_py_path):
        print(f"\nError: file {bot_py_path} not found")
        return
    
    try:
        # Create backup of bot.py
        backup_path = bot_py_path + '.multiline_fix.bak'
        with open(bot_py_path, 'r', encoding='utf-8') as f_in, open(backup_path, 'w', encoding='utf-8') as f_out:
            f_out.write(f_in.read())
        
        print(f"  - Created backup at {backup_path}")
        
        # Read the file content
        with open(bot_py_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Find the show_stats function
        pattern = re.compile(r'def show_stats\(update: Update, context: CallbackContext\) -> None:(.*?)(?=\n\ndef)', re.DOTALL)
        match = pattern.search(content)
        
        if not match:
            print("  - Could not find the show_stats function")
            return
        
        # Get the function body
        function_body = match.group(1)
        
        # Fix the multiline strings
        # Replace problematic string assignments with triple-quoted strings
        fixed_body = function_body
        
        # Fix the basic statistics section
        fixed_body = re.sub(
            r'stats_text = "\*Статистика бота\*\s*\n\s*"',
            'stats_text = """*Статистика бота*\n\n"""',
            fixed_body
        )
        
        # Fix user count string
        fixed_body = re.sub(
            r'stats_text \+= f"Всего пользователей: {total_users}\s*\n\s*"',
            'stats_text += f"""Всего пользователей: {total_users}\n"""',
            fixed_body
        )
        
        # Fix active users string
        fixed_body = re.sub(
            r'stats_text \+= f"Запустили бота: {len\(active_users\)}\s*\n\s*"',
            'stats_text += f"""Запустили бота: {len(active_users)}\n"""',
            fixed_body
        )
        
        # Fix admins string
        fixed_body = re.sub(
            r'stats_text \+= f"Администраторов: {total_admins}\s*\n\s*\n\s*"',
            'stats_text += f"""Администраторов: {total_admins}\n\n"""',
            fixed_body
        )
        
        # Fix inactive users string
        fixed_body = re.sub(
            r'stats_text \+= f"Добавлено, но не запустили бота: {inactive_users}\s*\n\s*\n\s*"',
            'stats_text += f"""Добавлено, но не запустили бота: {inactive_users}\n\n"""',
            fixed_body
        )
        
        # Fix latest video header
        fixed_body = re.sub(
            r'stats_text \+= f"\*Запись занятия 18 мая получили \({latest_video_unique_users}\):\*\s*\n\s*"',
            'stats_text += f"""*Запись занятия 18 мая получили ({latest_video_unique_users}):*\n"""',
            fixed_body
        )
        
        # Fix user display in latest video section
        fixed_body = re.sub(
            r'stats_text \+= f"- {user_display} {time_display}\s*\n\s*"',
            'stats_text += f"""- {user_display} {time_display}\n"""',
            fixed_body
        )
        
        # Fix no users message for latest video
        fixed_body = re.sub(
            r'stats_text \+= "Никто еще не получил запись\.\s*\n\s*"',
            'stats_text += """Никто еще не получил запись.\n"""',
            fixed_body
        )
        
        # Fix previous video header
        fixed_body = re.sub(
            r'stats_text \+= f"\s*\n\*Запись занятия 22 мая получили \({previous_video_unique_users}\):\*\s*\n\s*"',
            'stats_text += f"""\n*Запись занятия 22 мая получили ({previous_video_unique_users}):*\n"""',
            fixed_body
        )
        
        # Fix no users message for previous video
        fixed_body = re.sub(
            r'stats_text \+= "Никто еще не получил запись\.\s*\n\s*"',
            'stats_text += """Никто еще не получил запись.\n"""',
            fixed_body
        )
        
        # Replace the function body in the content
        new_content = pattern.sub(f'def show_stats(update: Update, context: CallbackContext) -> None:{fixed_body}', content)
        
        # Write the updated content back to the file
        with open(bot_py_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        print("  - Successfully fixed multiline strings in show_stats function")
        print("  - The /stats command should now work without syntax errors")
        
    except Exception as e:
        print(f"\nError fixing multiline strings: {e}")
    
    print("\n===== FIXING MULTILINE STRINGS COMPLETED =====\n")

def main():
    fix_multiline_strings()

if __name__ == "__main__":
    main()
