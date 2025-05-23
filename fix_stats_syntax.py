#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re

def fix_stats_syntax():
    """
    u0418u0441u043fu0440u0430u0432u043bu044fu0435u0442 u0441u0438u043du0442u0430u043au0441u0438u0447u0435u0441u043au0443u044e u043eu0448u0438u0431u043au0443 u0432 u0444u0443u043du043au0446u0438u0438 show_stats
    """
    print("\n===== u0418u0421u041fu0420u0410u0412u041bu0415u041du0418u0415 u0421u0418u041du0422u0410u041au0421u0418u0427u0415u0421u041au041eu0419 u041eu0428u0418u0411u041au0418 u0412 /stats =====\n")
    
    # u041fu0443u0442u044c u043a u0444u0430u0439u043bu0443 bot.py
    bot_py_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bot.py')
    
    # u041fu0440u043eu0432u0435u0440u044fu0435u043c, u0441u0443u0449u0435u0441u0442u0432u0443u0435u0442 u043bu0438 u0444u0430u0439u043b
    if not os.path.exists(bot_py_path):
        print(f"\nu041eu0448u0438u0431u043au0430: u0444u0430u0439u043b {bot_py_path} u043du0435 u043du0430u0439u0434u0435u043d")
        return
    
    try:
        # u0421u043eu0437u0434u0430u0435u043c u0440u0435u0437u0435u0440u0432u043du0443u044e u043au043eu043fu0438u044e bot.py
        backup_path = bot_py_path + '.syntax_fix.bak'
        with open(bot_py_path, 'r', encoding='utf-8') as f_in, open(backup_path, 'w', encoding='utf-8') as f_out:
            f_out.write(f_in.read())
        
        print(f"  - u0421u043eu0437u0434u0430u043du0430 u0440u0435u0437u0435u0440u0432u043du0430u044f u043au043eu043fu0438u044f {backup_path}")
        
        # u0427u0438u0442u0430u0435u043c u0444u0430u0439u043b bot.py
        with open(bot_py_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # u0418u0441u043fu0440u0430u0432u043bu044fu0435u043c u043fu0440u043eu0431u043bu0435u043cu043du044bu0439 u0443u0447u0430u0441u0442u043eu043a u0441 f-u0441u0442u0440u043eu043au0430u043cu0438
        for i in range(len(lines)):
            if "stats_text = (" in lines[i]:
                # u041du0430u0445u043eu0434u0438u043c u043du0430u0447u0430u043bu043e u043fu0440u043eu0431u043bu0435u043cu043du043eu0433u043e u0431u043bu043eu043au0430
                start_idx = i
                # u041du0430u0445u043eu0434u0438u043c u043au043eu043du0435u0446 u043fu0440u043eu0431u043bu0435u043cu043du043eu0433u043e u0431u043bu043eu043au0430
                end_idx = i
                while end_idx < len(lines) and ')' not in lines[end_idx]:
                    end_idx += 1
                
                # u0417u0430u043cu0435u043du044fu0435u043c u043fu0440u043eu0431u043bu0435u043cu043du044bu0439 u0431u043bu043eu043a u043du0430 u0438u0441u043fu0440u0430u0432u043bu0435u043du043du044bu0439
                fixed_block = [
                    "    # Format basic statistics\n",
                    "    stats_text = (\n",
                    "        '*u0421u0442u0430u0442u0438u0441u0442u0438u043au0430 u0431u043eu0442u0430*\\n\\n'\n",
                    "        f'u0412u0441u0435u0433u043e u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439: {total_users}\\n'\n",
                    "        f'u0417u0430u043fu0443u0441u0442u0438u043bu0438 u0431u043eu0442u0430: {len(active_users)}\\n'\n",
                    "        f'u0410u0434u043cu0438u043du0438u0441u0442u0440u0430u0442u043eu0440u043eu0432: {total_admins}\\n\\n'\n",
                    "    )\n"
                ]
                
                # u0417u0430u043cu0435u043du044fu0435u043c u0441u0442u0440u043eu043au0438
                lines[start_idx-1:end_idx+1] = fixed_block
                print("  - u0418u0441u043fu0440u0430u0432u043bu0435u043d u0431u043bu043eu043a u0441 f-u0441u0442u0440u043eu043au0430u043cu0438 u0432 u0444u0443u043du043au0446u0438u0438 show_stats")
                break
        
        # u0417u0430u043fu0438u0441u044bu0432u0430u0435u043c u0438u0441u043fu0440u0430u0432u043bu0435u043du043du044bu0439 u0444u0430u0439u043b
        with open(bot_py_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        print("  - u0418u0441u043fu0440u0430u0432u043bu0435u043du0430 u0441u0438u043du0442u0430u043au0441u0438u0447u0435u0441u043au0430u044f u043eu0448u0438u0431u043au0430 u0432 u0444u0443u043du043au0446u0438u0438 show_stats")
        print("  - u0422u0435u043fu0435u0440u044c u043au043eu043cu0430u043du0434u0430 /stats u0434u043eu043bu0436u043du0430 u0440u0430u0431u043eu0442u0430u0442u044c u0431u0435u0437 u043eu0448u0438u0431u043eu043a")
        
    except Exception as e:
        print(f"\nu041eu0448u0438u0431u043au0430 u043fu0440u0438 u0438u0441u043fu0440u0430u0432u043bu0435u043du0438u0438 u0441u0438u043du0442u0430u043au0441u0438u0447u0435u0441u043au043eu0439 u043eu0448u0438u0431u043au0438: {e}")
    
    print("\n===== u0418u0421u041fu0420u0410u0412u041bu0415u041du0418u0415 u0421u0418u041du0422u0410u041au0421u0418u0427u0415u0421u041au041eu0419 u041eu0428u0418u0411u041au0418 u0417u0410u0412u0415u0420u0428u0415u041du041e =====\n")

def main():
    fix_stats_syntax()

if __name__ == "__main__":
    main()
