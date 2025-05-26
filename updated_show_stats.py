def show_stats(update: Update, context: CallbackContext) -> None:
    """Показать статистику использования бота"""
    # Проверяем, является ли пользователь администратором
    user_id = update.effective_user.id
    if not is_admin(user_id):
        update.message.reply_text("Эта команда доступна только администраторам.")
        return

    # Подключаемся к базе данных
    conn = get_db_connection()
    cursor = conn.cursor()

    # Определяем тип базы данных
    db_type = 'sqlite'
    try:
        cursor.execute("SELECT version()")
        db_type = 'postgres'
    except:
        pass

    # Получаем общее количество пользователей
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]

    # Получаем количество активированных пользователей
    cursor.execute("SELECT COUNT(*) FROM users WHERE is_active = 1")
    active_users = cursor.fetchone()[0]

    # Получаем количество администраторов
    cursor.execute("SELECT COUNT(*) FROM users WHERE is_admin = 1")
    admin_count = cursor.fetchone()[0]

    # Получаем количество неактивированных пользователей
    inactive_users = total_users - active_users

    # Формируем текст статистики
    stats_text = f"Статистика бота:\n"
    stats_text += f"Всего пользователей: {total_users}\n"
    stats_text += f"Запустили бота: {active_users}\n"
    stats_text += f"Администраторов: {admin_count}\n"
    stats_text += f"Добавлено, но не запустили бота: {inactive_users}\n\n"

    # Получаем список администраторов
    cursor.execute("SELECT username, first_name, last_name FROM users WHERE is_admin = 1")
    admins = cursor.fetchall()

    # Добавляем список администраторов
    stats_text += "Список администраторов:\n"
    for admin in admins:
        username, first_name, last_name = admin
        if username:
            admin_display = "@" + username
        else:
            admin_display = first_name + (" " + last_name if last_name else "")
        stats_text += f"- {admin_display}\n"

    stats_text += "\n"

    # Получаем статистику по видео
    # Сначала получаем все действия, связанные с получением видео
    if db_type == 'postgres':
        cursor.execute("""
            SELECT action, COUNT(*) 
            FROM logs 
            WHERE action LIKE 'get_video%' OR action = 'get_latest_video' OR action = 'get_previous_video'
            GROUP BY action
        """)
    else:
        cursor.execute("""
            SELECT action, COUNT(*) 
            FROM logs 
            WHERE action LIKE 'get_video%' OR action = 'get_latest_video' OR action = 'get_previous_video'
            GROUP BY action
        """)

    video_actions = cursor.fetchall()

    # Получаем даты занятий из базы данных
    video_dates = {}
    try:
        # Запрашиваем даты занятий из базы данных
        cursor.execute("SELECT id, date FROM videos")
        dates = cursor.fetchall()
        
        # Формируем словарь соответствия идентификаторов и дат
        for i, date in dates:
            if i == 0:
                video_dates['get_latest_video'] = date
            elif i == 1:
                video_dates['get_previous_video'] = date
            else:
                video_dates[f'get_video_{i}'] = date
    except Exception as e:
        print(f"Ошибка при получении дат занятий: {e}")
    
    # Получаем информацию о соответствии действий и дат из базы данных
    action_to_date = {}
    try:
        # Запрашиваем данные из логов, где есть информация о записях занятий
        cursor.execute("""
            SELECT DISTINCT action, action_data 
            FROM logs 
            WHERE action_data LIKE 'Запись занятия%'
            OR action LIKE 'get_video_%'
            OR action IN ('get_latest_video', 'get_previous_video')
        """)
        
        action_date_mapping = cursor.fetchall()
        
        # Создаем словарь соответствия действий и дат
        for action, action_data in action_date_mapping:
            if action_data and 'Запись занятия' in action_data:
                # Извлекаем дату из action_data (например, "Запись занятия 22 мая")
                date = action_data.replace('Запись занятия ', '')
                action_to_date[action] = date
    except Exception as e:
        print(f"Ошибка при получении соответствия действий и дат: {e}")
    
    # Устанавливаем соответствие по умолчанию для основных действий, если не найдено в базе
    if 'get_latest_video' not in action_to_date:
        action_to_date['get_latest_video'] = '25 мая'
    if 'get_previous_video' not in action_to_date:
        action_to_date['get_previous_video'] = '22 мая'
    
    # Создаем словарь для хранения действий по датам
    date_to_actions = {
        '18 мая': [],
        '22 мая': [],
        '25 мая': []
    }
    
    # Распределяем действия по датам на основе данных из базы
    for action, _ in video_actions:
        # Если у нас есть информация о дате для этого действия из базы
        if action in action_to_date:
            date = action_to_date[action]
            if date in date_to_actions:
                date_to_actions[date].append(action)
        # Если нет информации из базы, используем логику по умолчанию
        else:
            # Действия для 25 мая
            if action == 'get_latest_video' or action == 'get_video_25 мая' or '25 мая' in action:
                date_to_actions['25 мая'].append(action)
            # Действия для 22 мая
            elif action == 'get_previous_video' or action == 'get_video_22 мая' or '22 мая' in action:
                date_to_actions['22 мая'].append(action)
            # Действия для 18 мая
            elif action == 'get_video_2' or action == 'get_video_18 мая' or '18 мая' in action:
                date_to_actions['18 мая'].append(action)
    
    # Добавляем фиктивное действие для 18 мая, если нет действий, чтобы всегда показывать эту дату
    if not date_to_actions['18 мая']:
        video_actions.append(('get_video_18 мая', 0))
        date_to_actions['18 мая'].append('get_video_18 мая')
    
    # Жестко задаем порядок отображения дат
    ordered_dates = ['18 мая', '22 мая', '25 мая']
    
    # Обрабатываем каждую дату в заданном порядке
    for date in ordered_dates:
        # Получаем список действий для этой даты
        actions_for_date = date_to_actions[date]
        
        # Если есть действия для этой даты или это 18 мая (которое всегда показываем)
        if actions_for_date or date == '18 мая':
            # Если у нас есть действия для этой даты, формируем запрос с использованием IN
            if actions_for_date:
                # Для нескольких действий используем IN с параметрами
                placeholders = ', '.join(['%s' if db_type == 'postgres' else '?'] * len(actions_for_date))
                
                if db_type == 'postgres':
                    query = f"""
                        SELECT DISTINCT l.username, l.user_id, u.first_name, u.last_name 
                        FROM logs l
                        LEFT JOIN users u ON l.user_id = u.user_id
                        WHERE l.action IN ({placeholders}) 
                        ORDER BY l.username
                    """
                else:
                    query = f"""
                        SELECT DISTINCT l.username, l.user_id, u.first_name, u.last_name 
                        FROM logs l
                        LEFT JOIN users u ON l.user_id = u.user_id
                        WHERE l.action IN ({placeholders}) 
                        ORDER BY l.username
                    """
                
                cursor.execute(query, actions_for_date)
            else:
                # Если нет действий, но это 18 мая, используем пустой запрос (вернет 0 пользователей)
                if db_type == 'postgres':
                    cursor.execute("""
                        SELECT DISTINCT l.username, l.user_id, u.first_name, u.last_name 
                        FROM logs l
                        LEFT JOIN users u ON l.user_id = u.user_id
                        WHERE 1=0 -- Пустой результат
                        ORDER BY l.username
                    """)
                else:
                    cursor.execute("""
                        SELECT DISTINCT l.username, l.user_id, u.first_name, u.last_name 
                        FROM logs l
                        LEFT JOIN users u ON l.user_id = u.user_id
                        WHERE 1=0 -- Пустой результат
                        ORDER BY l.username
                    """)
            
            video_users = cursor.fetchall()
            
            # Добавляем статистику для этой даты
            stats_text += f"Запись занятия {date} получили: {len(video_users)}\n"
            
            # Добавляем список пользователей
            for user in video_users:
                username, user_id, first_name, last_name = user
                # Формируем отображаемое имя пользователя
                if username and username != 'admin':
                    user_display = "@" + username
                    # Добавляем имя пользователя, если оно есть
                    if first_name:
                        full_name = first_name + (" " + last_name if last_name else "")
                        user_display += f" ({full_name})"
                elif first_name:
                    full_name = first_name + (" " + last_name if last_name else "")
                    user_display = full_name + f" (ID: {user_id})"
                else:
                    user_display = "ID: " + str(user_id)
                stats_text += "- " + user_display + "\n"
            
            # Добавляем пустую строку после каждой группы
            stats_text += "\n"
    
    # Send statistics
    update.message.reply_text(stats_text)
    
    # Close database connection
    cursor.close()
    conn.close()
