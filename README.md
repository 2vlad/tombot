# TomBot - Telegram Bot for Film School

A simple Telegram bot that provides film school students with access to video recordings of the last two lessons.

## Features

- Two main buttons for accessing video links:
  - "Last Lesson" - sends a link to the most recent video recording
  - "Previous Lesson" - sends a link to the second most recent video recording
- Authorization system that only allows registered students (by Telegram ID) to access bot functions
- Admin commands for managing students and video links
- Statistics tracking for bot usage

## Deployment on Railway

### Prerequisites

- A GitHub account
- A Railway account (sign up at [railway.app](https://railway.app/))
- A Telegram bot token (obtained from [@BotFather](https://t.me/botfather))

### Deployment Steps

1. **Fork or Clone this Repository to GitHub**
   - Create a new GitHub repository
   - Upload all the files from this project to your repository

2. **Sign up for Railway**
   - Go to [railway.app](https://railway.app/) and sign up using your GitHub account
   - Create a new project

3. **Deploy from GitHub**
   - In Railway, click "New Project"
   - Select "Deploy from GitHub repo"
   - Choose your repository

4. **Set Environment Variables**
   - In your Railway project, go to the "Variables" tab
   - Add the following variables:
     - `TELEGRAM_TOKEN`: Your Telegram bot token (7937927576:AAHVQm4AGYNWG6BD-ZNWSfn9XpAb-9wU1dw)
     - `ADMIN_ID`: Your Telegram user ID (89118240)
     - `BUTTON1_TEXT`: Text for the first button (e.g., 'Запись занятия 18 мая')
     - `BUTTON1_MESSAGE`: Message text for the first button (e.g., 'Запись занятия: https://drive.google.com/...')
     - `BUTTON2_TEXT`: Text for the second button (e.g., 'Запись занятия 22 мая')
     - `BUTTON2_MESSAGE`: Message text for the second button (e.g., 'Запись занятия: https://drive.google.com/...')

   > **Важно:** После изменения кнопок через команды `/button1` или `/button2`, необходимо обновить эти переменные в панели Railway, чтобы изменения сохранились после перезапуска бота. Значения можно найти в файле `.env` в корне проекта после выполнения команд.

5. **Add Persistent Storage**
   - Go to the "Plugins" tab
   - Add the "Volume" plugin
   - This will create a persistent storage volume for your database

6. **Deploy Your Bot**
   - Railway will automatically deploy your bot
   - You can view logs in the "Deployments" tab

7. **Verify Bot is Running**
   - Open Telegram and message your bot
   - Try the /start command to verify it's working

### Local Setup

1. Clone this repository
2. Install the required packages:

```bash
pip install -r requirements.txt
```

3. Set up environment variables:

```bash
export TELEGRAM_TOKEN="your_telegram_bot_token"
export ADMIN_ID="your_telegram_id"  # Optional: set the admin Telegram ID
```

### Running the Bot

```bash
python bot.py
```

## Admin Commands

- `/adduser <user_id>` - Add a new user by their Telegram ID
- `/removeuser <user_id>` - Remove a user by their Telegram ID
- `/updatevideo <number> <title> <url>` - Update video link (1 for latest, 2 for previous)
- `/stats` - Show bot usage statistics

## User Commands

- `/start` - Start the bot and show the main keyboard
- `/help` - Show available commands

## Database Structure

The bot uses SQLite to store:

- Authorized users
- Video links with dates
- Usage logs

## License

MIT
