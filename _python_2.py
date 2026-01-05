import os
import logging

# Настройка логирования
logger = logging.getLogger(__name__)

# Проверяем наличие psycopg2
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    logger.warning("psycopg2 не установлен. Используется режим без БД.")

# Если нет PostgreSQL, используем SQLite
if not PSYCOPG2_AVAILABLE:
    try:
        import sqlite3
        SQLITE_AVAILABLE = True
        logger.info("Используется SQLite как fallback")
    except ImportError:
        SQLITE_AVAILABLE = False
        logger.error("Нет доступной базы данных!")

# Конфигурация БД
DATABASE_URL = os.getenv("DATABASE_URL")

# Флаг использования SQLite
USE_SQLITE = False

if not DATABASE_URL and PSYCOPG2_AVAILABLE:
    # Если нет PostgreSQL, но есть SQLite
    if SQLITE_AVAILABLE:
        USE_SQLITE = True
        DATABASE_URL = "dota2_bot.db"
        logger.info("Используется SQLite база данных")
    else:
        logger.error("Нет настроек базы данных!")

def get_conn():
    """Получение соединения с БД"""
    if USE_SQLITE and SQLITE_AVAILABLE:
        conn = sqlite3.connect(DATABASE_URL)
        conn.row_factory = sqlite3.Row  # Для dict-like результатов
        return conn
    elif PSYCOPG2_AVAILABLE and DATABASE_URL:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    else:
        raise Exception("Нет доступной базы данных")

def init_db():
    """Инициализация базы данных"""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        if USE_SQLITE:
            # SQLite таблицы
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id BIGINT PRIMARY KEY,
                    account_id BIGINT NOT NULL,
                    score INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS friends (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id BIGINT,
                    friend_account_id BIGINT NOT NULL,
                    friend_name TEXT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(telegram_id)
                )
            """)
        else:
            # PostgreSQL таблицы
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id BIGINT PRIMARY KEY,
                    account_id BIGINT NOT NULL,
                    score INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS friends (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES users(telegram_id),
                    friend_account_id BIGINT NOT NULL,
                    friend_name TEXT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("✅ База данных инициализирована")
        
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")
        raise

def bind_user(telegram_id, account_id):
    """Привязка пользователя к аккаунту Steam"""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        if USE_SQLITE:
            cur.execute("""
                INSERT OR REPLACE INTO users (telegram_id, account_id)
                VALUES (?, ?)
            """, (telegram_id, account_id))
        else:
            cur.execute("""
                INSERT INTO users (telegram_id, account_id)
                VALUES (%s, %s)
                ON CONFLICT (telegram_id) 
                DO UPDATE SET account_id = EXCLUDED.account_id
            """, (telegram_id, account_id))
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Пользователь {telegram_id} привязан к аккаунту {account_id}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка привязки пользователя: {e}")
        return False

def get_account_id(telegram_id):
    """Получение account_id пользователя"""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        if USE_SQLITE:
            cur.execute("SELECT account_id FROM users WHERE telegram_id = ?", (telegram_id,))
        else:
            cur.execute("SELECT account_id FROM users WHERE telegram_id = %s", (telegram_id,))
        
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row:
            # Для SQLite row_factory=sqlite3.Row, для psycopg2 RealDictCursor
            return dict(row)['account_id'] if row else None
        return None
        
    except Exception as e:
        logger.error(f"Ошибка получения account_id: {e}")
        return None

def add_friend(telegram_id, friend_account_id, friend_name):
    """Добавление друга"""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        if USE_SQLITE:
            cur.execute("""
                INSERT INTO friends (user_id, friend_account_id, friend_name)
                VALUES (?, ?, ?)
            """, (telegram_id, friend_account_id, friend_name))
        else:
            cur.execute("""
                INSERT INTO friends (user_id, friend_account_id, friend_name)
                VALUES (%s, %s, %s)
            """, (telegram_id, friend_account_id, friend_name))
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Пользователь {telegram_id} добавил друга {friend_name}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка добавления друга: {e}")
        return False

def get_friends(telegram_id):
    """Получение списка друзей"""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        if USE_SQLITE:
            cur.execute("""
                SELECT friend_account_id, friend_name 
                FROM friends 
                WHERE user_id = ?
                ORDER BY added_at DESC
            """, (telegram_id,))
        else:
            cur.execute("""
                SELECT friend_account_id, friend_name 
                FROM friends 
                WHERE user_id = %s
                ORDER BY added_at DESC
            """, (telegram_id,))
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        # Конвертируем в список словарей
        result = []
        for row in rows:
            result.append(dict(row))
        return result
        
    except Exception as e:
        logger.error(f"Ошибка получения друзей: {e}")
        return []

def update_score(telegram_id, points):
    """Обновление счета пользователя"""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        if USE_SQLITE:
            cur.execute("""
                UPDATE users 
                SET score = score + ? 
                WHERE telegram_id = ?
            """, (points, telegram_id))
        else:
            cur.execute("""
                UPDATE users 
                SET score = score + %s 
                WHERE telegram_id = %s
            """, (points, telegram_id))
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Обновлен счет пользователя {telegram_id}: +{points}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка обновления счета: {e}")
        return False

def get_leaderboard(limit=10):
    """Получение таблицы лидеров"""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        if USE_SQLITE:
            cur.execute("""
                SELECT telegram_id, score 
                FROM users 
                ORDER BY score DESC 
                LIMIT ?
            """, (limit,))
        else:
            cur.execute("""
                SELECT telegram_id, score 
                FROM users 
                ORDER BY score DESC 
                LIMIT %s
            """, (limit,))
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        # Конвертируем в список словарей
        result = []
        for row in rows:
            result.append(dict(row))
        return result
        
    except Exception as e:
        logger.error(f"Ошибка получения лидерборда: {e}")
        return []

def get_user_stats(telegram_id):
    """Получение статистики пользователя"""
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        if USE_SQLITE:
            cur.execute("""
                SELECT u.telegram_id, u.score, u.created_at,
                       COUNT(f.id) as friends_count
                FROM users u
                LEFT JOIN friends f ON u.telegram_id = f.user_id
                WHERE u.telegram_id = ?
                GROUP BY u.telegram_id
            """, (telegram_id,))
        else:
            cur.execute("""
                SELECT u.telegram_id, u.score, u.created_at,
                       COUNT(f.id) as friends_count
                FROM users u
                LEFT JOIN friends f ON u.telegram_id = f.user_id
                WHERE u.telegram_id = %s
                GROUP BY u.telegram_id
            """, (telegram_id,))
        
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        return dict(row) if row else None
        
    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}")
        return None