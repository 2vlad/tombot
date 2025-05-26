#!/usr/bin/env python
# -*- coding: utf-8 -*-

from video_analyzer import VideoDownloadsAnalyzer
import os
import json

def main():
    """u0413u043bu0430u0432u043du0430u044f u0444u0443u043du043au0446u0438u044f u0434u043bu044f u0434u0435u043cu043eu043du0441u0442u0440u0430u0446u0438u0438 u0438u0441u043fu043eu043bu044cu0437u043eu0432u0430u043du0438u044f"""
    
    # u0412u0430u0440u0438u0430u043du0442 1: u041fu043eu0434u043au043bu044eu0447u0435u043du0438u0435 u043a PostgreSQL (u0434u043bu044f Railway/Heroku/etc)
    if os.getenv('DATABASE_URL'):
        # u0415u0441u043bu0438 u0435u0441u0442u044c DATABASE_URL (u043au0430u043a u0432 Railway)
        import urllib.parse as urlparse
        
        url = urlparse.urlparse(os.getenv('DATABASE_URL'))
        
        analyzer = VideoDownloadsAnalyzer(
            db_type='postgresql',
            host=url.hostname,
            database=url.path[1:],  # u0443u0431u0438u0440u0430u0435u043c u043fu0435u0440u0432u044bu0439 u0441u043bu044du0448
            user=url.username,
            password=url.password,
            port=url.port
        )
    
    # u0412u0430u0440u0438u0430u043du0442 2: u041fu043eu0434u043au043bu044eu0447u0435u043du0438u0435 u043a PostgreSQL u0441 u043eu0442u0434u0435u043bu044cu043du044bu043cu0438 u043fu0435u0440u0435u043cu0435u043du043du044bu043cu0438
    elif all([os.getenv('DB_HOST'), os.getenv('DB_NAME'), os.getenv('DB_USER')]):
        analyzer = VideoDownloadsAnalyzer(
            db_type='postgresql',
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=int(os.getenv('DB_PORT', 5432))
        )
    
    # u0412u0430u0440u0438u0430u043du0442 3: SQLite u0434u043bu044f u043bu043eu043au0430u043bu044cu043du043eu0439 u0440u0430u0437u0440u0430u0431u043eu0442u043au0438
    else:
        analyzer = VideoDownloadsAnalyzer(
            db_type='sqlite',
            database='filmschool.db'  # u0418u0441u043fu043eu043bu044cu0437u0443u0435u043c u043bu043eu043au0430u043bu044cu043du0443u044e u0431u0430u0437u0443 u0434u0430u043du043du044bu0445
        )
    
    try:
        # u041eu0441u043du043eu0432u043du043eu0439 u0430u043du0430u043bu0438u0437 u0441u043au0430u0447u0438u0432u0430u043du0438u0439
        print("ud83cudfa5 u0410u041du0410u041bu0418u0417 u0421u041au0410u0427u0418u0412u0410u041du0418u0419 u0412u0418u0414u0415u041e u0417u0410u041fu0418u0421u0415u0419")
        print("=" * 50)
        
        # u041fu043eu043bu0443u0447u0430u0435u043c u043eu0442u0447u0435u0442 u0432 u043au043eu043du0441u043eu043bu044cu043du043eu043c u0444u043eu0440u043cu0430u0442u0435
        downloads_report = analyzer.analyze_downloads('console')
        print(downloads_report)
        
        # u0422u043eu043f u0430u043au0442u0438u0432u043du044bu0445 u043fu043eu043bu044cu0437u043eu0432u0430u0442u0435u043bu0435u0439
        print("\nud83dudcca u0422u041eu041f u0410u041au0422u0418u0412u041du042bu0425 u041fu041eu041bu042cu0417u041eu0412u0410u0422u0415u041bu0415u0419")
        print("=" * 50)
        top_users = analyzer.get_top_active_users(15)
        print(top_users)
        
        # u0414u043eu043fu043eu043bu043du0438u0442u0435u043bu044cu043du043e: u043eu0442u0447u0435u0442 u0432 JSON u0434u043bu044f API
        json_report = analyzer.analyze_downloads('json')
        
        # u0421u043eu0445u0440u0430u043du044fu0435u043c JSON u043eu0442u0447u0435u0442 u0432 u0444u0430u0439u043b
        with open('video_downloads_report.json', 'w', encoding='utf-8') as f:
            f.write(json_report)
        
        print(f"\nu2705 JSON u043eu0442u0447u0435u0442 u0441u043eu0445u0440u0430u043du0435u043d u0432 video_downloads_report.json")
        
        # u041au0440u0430u0442u043au0438u0439 u0442u0435u043au0441u0442u043eu0432u044bu0439 u043eu0442u0447u0435u0442
        text_report = analyzer.analyze_downloads('text')
        with open('video_downloads_summary.txt', 'w', encoding='utf-8') as f:
            f.write(text_report)
        
        print(f"u2705 u041au0440u0430u0442u043au0438u0439 u043eu0442u0447u0435u0442 u0441u043eu0445u0440u0430u043du0435u043d u0432 video_downloads_summary.txt")
        
    except Exception as e:
        print(f"u274c u041eu0448u0438u0431u043au0430: {e}")
        print("u041fu0440u043eu0432u0435u0440u044cu0442u0435 u043du0430u0441u0442u0440u043eu0439u043au0438 u043fu043eu0434u043au043bu044eu0447u0435u043du0438u044f u043a u0431u0430u0437u0435 u0434u0430u043du043du044bu0445")
    finally:
        # u0417u0430u043au0440u044bu0432u0430u0435u043c u0441u043eu0435u0434u0438u043du0435u043du0438u0435
        analyzer.disconnect()

def get_specific_date_downloads(date_string: str):
    """
    u041fu043eu043bu0443u0447u0438u0442u044c u0441u043au0430u0447u0438u0432u0430u043du0438u044f u0434u043bu044f u043au043eu043du043au0440u0435u0442u043du043eu0439 u0434u0430u0442u044b
    
    Args:
        date_string: u0434u0430u0442u0430 u0432 u0444u043eu0440u043cu0430u0442u0435 "25 u043cu0430u044f", "22 u043cu0430u044f" u0438 u0442.u0434.
    """
    # u041du0430u0441u0442u0440u043eu0439u043au0430 u043fu043eu0434u043au043bu044eu0447u0435u043du0438u044f (u0430u043du0430u043bu043eu0433u0438u0447u043du043e main())
    analyzer = VideoDownloadsAnalyzer(
        db_type='sqlite',
        database='filmschool.db'  # u0418u0441u043fu043eu043bu044cu0437u0443u0435u043c u043bu043eu043au0430u043bu044cu043du0443u044e u0431u0430u0437u0443 u0434u0430u043du043du044bu0445
    )
    
    try:
        analyzer.connect()
        downloads = analyzer.get_video_downloads()
        
        if date_string in downloads:
            users = downloads[date_string]
            user_names = [user['display_name'] for user in users]
            print(f"ud83dudcf9 u0417u0430u043fu0438u0441u044c {date_string} u0441u043au0430u0447u0430u043bu0438:")
            print(f"   {', '.join(user_names)}")
            print(f"   u0412u0441u0435u0433u043e: {len(users)} u0447u0435u043bu043eu0432u0435u043a")
            return user_names
        else:
            print(f"u274c u0414u0430u043du043du044bu0435 u0434u043bu044f u0437u0430u043fu0438u0441u0438 {date_string} u043du0435 u043du0430u0439u0434u0435u043du044b")
            return []
            
    finally:
        analyzer.disconnect()

# u041fu0440u0438u043cu0435u0440 u0434u043bu044f Flask/FastAPI u043fu0440u0438u043bu043eu0436u0435u043du0438u044f
def create_api_endpoint():
    """u041fu0440u0438u043cu0435u0440 u0441u043eu0437u0434u0430u043du0438u044f API endpoint u0434u043bu044f u043fu043eu043bu0443u0447u0435u043du0438u044f u0434u0430u043du043du044bu0445"""
    
    def get_downloads_api():
        """u0424u0443u043du043au0446u0438u044f API u0434u043bu044f u043fu043eu043bu0443u0447u0435u043du0438u044f u0434u0430u043du043du044bu0445 u043e u0441u043au0430u0447u0438u0432u0430u043du0438u044fu0445"""
        try:
            # u0414u043bu044f u043fu0440u043eu0434u0430u043au0448u0435u043du0430 u0438u0441u043fu043eu043bu044cu0437u0443u0435u043c PostgreSQL
            if os.getenv('DATABASE_URL'):
                import urllib.parse as urlparse
                url = urlparse.urlparse(os.getenv('DATABASE_URL'))
                
                analyzer = VideoDownloadsAnalyzer(
                    db_type='postgresql',
                    host=url.hostname,
                    database=url.path[1:],
                    user=url.username,
                    password=url.password,
                    port=url.port
                )
            else:
                # u0414u043bu044f u043bu043eu043au0430u043bu044cu043du043eu0439 u0440u0430u0437u0440u0430u0431u043eu0442u043au0438 u0438u0441u043fu043eu043bu044cu0437u0443u0435u043c SQLite
                analyzer = VideoDownloadsAnalyzer(
                    db_type='sqlite',
                    database='filmschool.db'
                )
            
            # u0412u043eu0437u0432u0440u0430u0449u0430u0435u043c JSON u0434u043bu044f API
            json_report = analyzer.analyze_downloads('json')
            analyzer.disconnect()
            return json.loads(json_report)  # u041fu0440u0435u043eu0431u0440u0430u0437u0443u0435u043c u0441u0442u0440u043eu043au0443 JSON u0432 u043eu0431u044au0435u043au0442 Python
            
        except Exception as e:
            return {'error': str(e)}
    
    return get_downloads_api

if __name__ == "__main__":
    # u0417u0430u043fu0443u0441u043a u043eu0441u043du043eu0432u043du043eu0433u043e u0430u043du0430u043bu0438u0437u0430
    main()
    
    # u041fu0440u0438u043cu0435u0440 u043fu043eu043bu0443u0447u0435u043du0438u044f u0434u0430u043du043du044bu0445 u0434u043bu044f u043au043eu043du043au0440u0435u0442u043du043eu0439 u0434u0430u0442u044b
    print("\n" + "="*50)
    get_specific_date_downloads("25 u043cu0430u044f")
    get_specific_date_downloads("22 u043cu0430u044f")
    get_specific_date_downloads("18 u043cu0430u044f")
