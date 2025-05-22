#!/bin/bash

# Set token directly in script
TELEGRAM_TOKEN="7937927576:AAHVQm4AGYNWG6BD-ZNWSfn9XpAb-9wU1dw"
ADMIN_ID="89118240"

# Export variables for the bot to use
export TELEGRAM_TOKEN
export ADMIN_ID

# Run the bot
python3 bot.py
