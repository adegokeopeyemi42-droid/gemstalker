def main():
    threading.Thread(target=run_flask, daemon=True).start()
    logger.info('Flask started')

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler('start', start_cmd))
    app.add_handler(CommandHandler('scan', scan_cmd))
    app.add_handler(CommandHandler('status', status_cmd))
    app.add_handler(CommandHandler('calls', calls_cmd))

    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, msg_handler)
    )

    jq = app.job_queue

    jq.run_repeating(monitor_job, interval=20, first=5)
    jq.run_repeating(track_job, interval=120, first=60)

    logger.info('GemStalker running')

    app.run_polling()


if __name__ == '__main__':
    main()
