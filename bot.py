import os
import re
import time
import asyncio
import logging
import datetime
import threading

from collections import deque

import httpx
from flask import Flask

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

DEX_PROFILES = 'https://api.dexscreener.com/token-profiles/latest/v1'
DEX_BOOSTS = 'https://api.dexscreener.com/token-boosts/latest/v1'
DEX_TOKEN = 'https://api.dexscreener.com/latest/dex/tokens/'
PUMP_FUN = 'https://frontend-api.pump.fun/coins?limit=20&sort=created_timestamp&order=DESC'
SOL_RPC = 'https://api.mainnet-beta.solana.com'

http = httpx.AsyncClient(timeout=10)

flask_app = Flask(__name__)


@flask_app.route('/')
def health():
    return {
        'status': 'alive'
    }


def run_flask():
    flask_app.run(
        host='0.0.0.0',
        port=int(os.getenv('PORT', 8080))
    )


def main():
    threading.Thread(
        target=run_flask,
        daemon=True
    ).start()

    logger.info('Flask started')

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('scan', scan))
    app.add_handler(CommandHandler('status', status))
    app.add_handler(CommandHandler('calls', calls))

    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            msg_handler
        )
    )

    jq = app.job_queue

    jq.run_repeating(
        monitor_job,
        interval=20,
        first=5
    )

    jq.run_repeating(
        track_job,
        interval=120,
        first=60
    )

    logger.info('GemStalker running')

    app.run_polling(drop_pending_updates=True)


if __name__ == '__main__':
    main()
