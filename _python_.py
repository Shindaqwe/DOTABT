from flask import Flask
from threading import Thread
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/')
def home():
    """Основная страница для проверки работы"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Dota2 Bot Status</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                text-align: center;
                padding: 50px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
            }
            .container {
                background: rgba(0,0,0,0.7);
                padding: 30px;
                border-radius: 15px;
                display: inline-block;
            }
            h1 {
                font-size: 2.5em;
                margin-bottom: 20px;
            }
            .status {
                font-size: 1.5em;
                color: #4CAF50;
                font-weight: bold;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🤖 Dota2 Stats Bot</h1>
            <p class="status">✅ Бот активен и работает</p>
            <p>Этот сервер поддерживает работу Telegram бота 24/7</p>
            <p>Статус: <span style="color: #4CAF50;">Online</span></p>
        </div>
    </body>
    </html>
    """

@app.route('/health')
def health():
    """Эндпоинт для проверки здоровья"""
    return {"status": "ok", "service": "dota2-bot"}, 200

@app.route('/status')
def status():
    """Статус сервера"""
    import datetime
    return {
        "status": "running",
        "timestamp": datetime.datetime.now().isoformat(),
        "service": "Dota2 Telegram Bot"
    }

def run():
    """Запуск Flask сервера"""
    try:
        # Используем порт из переменной окружения или 8080 по умолчанию
        port = int(os.environ.get('PORT', 8080))
        logger.info(f"🚀 Запуск keep-alive сервера на порту {port}")
        app.run(host='0.0.0.0', port=port)
    except Exception as e:
        logger.error(f"❌ Ошибка запуска сервера: {e}")

def keep_alive():
    """Запуск сервера в отдельном потоке"""
    try:
        t = Thread(target=run, daemon=True)
        t.start()
        logger.info("✅ Keep-alive сервер запущен в фоновом режиме")
        return t
    except Exception as e:
        logger.error(f"❌ Ошибка запуска keep-alive: {e}")
        return None

# Для запуска напрямую
if __name__ == '__main__':
    import os
    run()