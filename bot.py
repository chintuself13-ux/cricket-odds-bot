import os
import sys
import time
import asyncio
import threading
import logging
import html
import json
import re
import urllib.request
import urllib.parse
import aiohttp
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, ThreadingHTTPServer, BaseHTTPRequestHandler
from typing import Dict, Any, Optional, List, Tuple

try:
    import pymongo
    HAS_PYMONGO = True
except ImportError:
    pymongo = None
    HAS_PYMONGO = False

from odds_engine import MultiTrackOddsEngine, TrackJob, global_odds_data_engine
from exchange_scraper import global_exchange_scraper, format_indian_odds, is_team_match, set_base_url, BASE_URL

ADMIN_ID = 7592394328


class RenderHealthCheckHandler(BaseHTTPRequestHandler):
    """
    Simple HTTP Request Handler to satisfy Render Web Service port scans and health checks.
    """
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK - Telegram Live Cricket Odds Bot is Active")

    def log_message(self, format, *args):
        pass  # Suppress HTTP server logs to keep console clean


def start_keep_alive_ping(port: int):
    """
    Background keep-alive thread that sends HTTP GET request to self
    every 120 seconds (2 minutes) to keep Render network sockets active.
    """
    def _ping_loop():
        ext_url = os.environ.get("RENDER_EXTERNAL_URL", "").strip().rstrip("/")
        if ext_url:
            ping_target = ext_url
        else:
            ping_target = f"http://127.0.0.1:{port}/"

        logger.info(f"Keep-alive self-ping task started targeting {ping_target}")
        
        while True:
            time.sleep(120)
            try:
                req = urllib.request.Request(
                    ping_target,
                    headers={"User-Agent": "Render-KeepAlive-Ping/1.0"}
                )
                with urllib.request.urlopen(req, timeout=10.0) as resp:
                    resp.read()
                logger.debug(f"Render self-ping success -> {ping_target}")
            except Exception as e:
                logger.debug(f"Render self-ping notice ({ping_target}): {e}")

    thread = threading.Thread(target=_ping_loop, daemon=True)
    thread.start()


def start_health_server(port: int):
    """Start background multithreaded HTTP health check server on 0.0.0.0:<port>."""
    try:
        server = ThreadingHTTPServer(("0.0.0.0", port), RenderHealthCheckHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        logger.info(f"Multithreaded health check HTTP server listening on 0.0.0.0:{port}")
        
        # Start lightweight background keep-alive self-ping loop every 2 minutes
        start_keep_alive_ping(port)
        return server
    except Exception as e:
        logger.warning(f"Failed to start health check HTTP server on port {port}: {e}")
        return None


class TelegramBotClient:
    """
    Lightweight Telegram Bot API client using urllib.
    Supports Long Polling (getUpdates), sendMessage, and sendPhoto.
    """
    def __init__(self, token: str):
        self.token = token.strip()
        self.base_url = f"https://api.telegram.org/bot{self.token}"

    def get_me(self) -> Tuple[bool, Dict[str, Any]]:
        """Verify bot token and fetch bot info."""
        return self._make_request("getMe")

    def get_updates(self, offset: Optional[int] = None, timeout: int = 25) -> Tuple[bool, List[Dict[str, Any]]]:
        """Fetch incoming updates via long polling."""
        params = {"timeout": timeout}
        if offset is not None:
            params["offset"] = offset
        ok, res = self._make_request("getUpdates", params=params, timeout=timeout + 10)
        if ok and isinstance(res, list):
            return True, res
        return False, []

    def send_message(
        self,
        chat_id: str | int,
        text: str,
        parse_mode: str = "HTML",
        disable_notification: bool = False,
        reply_markup: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """Send text message to a Telegram chat."""
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
            "disable_notification": disable_notification
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup

        return self._make_request("sendMessage", json_payload=payload)

    def send_photo(
        self,
        chat_id: str | int,
        photo: str,
        caption: str = "",
        parse_mode: str = "HTML"
    ) -> Tuple[bool, Dict[str, Any]]:
        """Send photo (via URL or file_id) to a Telegram chat."""
        payload = {
            "chat_id": chat_id,
            "photo": photo,
            "caption": caption,
            "parse_mode": parse_mode
        }
        return self._make_request("sendPhoto", json_payload=payload)

    def answer_callback_query(
        self,
        callback_query_id: str,
        text: str = "",
        show_alert: bool = False
    ) -> Tuple[bool, Dict[str, Any]]:
        """Answer callback query from inline keyboard button press."""
        payload = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
        if show_alert:
            payload["show_alert"] = True
        return self._make_request("answerCallbackQuery", json_payload=payload)

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_payload: Optional[Dict[str, Any]] = None,
        timeout: float = 10.0
    ) -> Tuple[bool, Any]:
        url = f"{self.base_url}/{endpoint}"
        try:
            if json_payload is not None:
                data = json.dumps(json_payload).encode("utf-8")
                req = urllib.request.Request(
                    url,
                    data=data,
                    headers={"Content-Type": "application/json"}
                )
            elif params:
                query_string = urllib.parse.urlencode(params)
                url = f"{url}?{query_string}"
                req = urllib.request.Request(url)
            else:
                req = urllib.request.Request(url)

            with urllib.request.urlopen(req, timeout=timeout) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                if result.get("ok"):
                    return True, result.get("result")
                else:
                    return False, result.get("description", "Telegram API Error")
        except Exception as e:
            return False, str(e)


# Global dictionary for instant 0ms /status response: ACTIVE_TRACKS[chat_id]
ACTIVE_TRACKS: Dict[Any, Dict[str, Dict[str, Any]]] = {}

# User state machine tracking for interactive Telegram inline flows: USER_STATES[user_id]
USER_STATES: Dict[int, Dict[str, Any]] = {}


def parse_target_odd_input(val_str: str) -> Optional[float]:
    """
    Parses user input for target odd.
    Handles both Indian ground paise inputs (e.g. '20' -> 1.20, '85' -> 1.85, '5' -> 1.05, '0.20' -> 1.20)
    and decimal odd inputs (e.g. '1.20' -> 1.20, '2.40' -> 2.40).
    Returns rounded float decimal odd or None if invalid.
    """
    s = val_str.strip().replace("₹", "").replace("$", "")
    try:
        val = float(s)
        if val <= 0:
            return None
        # If user entered fractional paise like 0.20 or .20 (< 1.0)
        if val < 1.0:
            val = 1.0 + val
        # If user entered integer paise without decimal point (e.g. '20', '85', '5', '115')
        elif "." not in s and val >= 2.0:
            val = 1.0 + (val / 100.0)
        return round(val, 2)
    except (ValueError, TypeError):
        return None



def parse_stake_input(val_str: str) -> Optional[float]:
    """
    Parses user input for stake amount.
    Returns float (0 or positive) or None if invalid.
    """
    s = val_str.strip().replace("₹", "").replace("$", "").replace(",", "")
    try:
        val = float(s)
        if val < 0:
            return None
        return round(val, 2)
    except (ValueError, TypeError):
        return None


def clean_short_team_name(name: str) -> str:
    """
    Strips long tournament suffixes and noise words (European T20, Caribbean Premier League, etc.)
    and truncates to clean short team name (Max 10-12 chars).
    """
    if not name:
        return ""
    s = name.strip()
    suffixes = [
        r'\bEuropean T20\b', r'\bCaribbean Premier League\b', r'\bPremier League\b', r'\bSuper League\b',
        r'\bQualifier\b', r'\bSeries\b', r'\bT20I\b', r'\bT20\b', r'\bT10\b', r'\bODI\b', r'\bTest\b',
        r'\bMatch\b', r'\bWomen\b', r'\bWomens\b', r'\bLeague\b', r'\bChallenge\b', r'\bTrophy\b',
        r'\bCup\b', r'\bShield\b', r'\bBlast\b'
    ]
    # Strip 'Of' constructions like 'Sri Lanka Of England' -> 'Sri Lanka'
    s = re.sub(r'\s+of\s+.*$', '', s, flags=re.IGNORECASE).strip()
    for pat in suffixes:
        s = re.sub(pat, '', s, flags=re.IGNORECASE).strip()
    s = re.sub(r'\s+', ' ', s).strip()

    words = s.split()
    if len(words) > 1:
        s = " ".join(words[:2])
    if len(s) > 11 and len(words) > 0:
        s = words[0]
    if len(s) > 11:
        s = s[:11].strip()
    return s if s else name[:11].strip()


def format_short_match_button_label(home_team: str, away_team: str) -> str:
    """
    Formats match button text for 2-column grid layout (Max 20-24 characters).
    Format: '🏏 TeamA vs TeamB'
    """
    t1 = clean_short_team_name(home_team)
    t2 = clean_short_team_name(away_team)
    label = f"🏏 {t1} vs {t2}"
    if len(label) > 24:
        label = label[:23] + "…"
    return label


# Force UTF-8 encoding for Windows console output
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("OddsBot")

# Load environment variables if python-dotenv installed
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8903237867:AAFkZV59PF6_9ChXh7S5b9_AfsrG9tLUT-o").strip()

DEFAULT_ADMIN_ID = 7592394328
ALLOWED_USERS_FILE = "allowed_users.json"
ACTIVE_JOBS_FILE = "active_jobs.json"

MONGO_URI = os.environ.get("MONGO_URI", "").strip()

_mongo_db_instance = None
_mongo_init_attempted = False


def get_mongo_db():
    global _mongo_db_instance, _mongo_init_attempted
    if not HAS_PYMONGO or not MONGO_URI:
        return None
    if _mongo_db_instance is not None:
        return _mongo_db_instance

    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=4000)
        client.admin.command('ping')
        try:
            db = client.get_default_database(default="track_odds_db")
        except Exception:
            db = client["track_odds_db"]
        _mongo_db_instance = db
        logger.info("Successfully connected to MongoDB storage!")
        return db
    except Exception as e:
        if not _mongo_init_attempted:
            logger.warning(f"MongoDB connection notice ({e}). Using local JSON fallback.")
            _mongo_init_attempted = True
        return None


def load_allowed_users() -> Dict[int, Optional[datetime]]:
    allowed: Dict[int, Optional[datetime]] = {DEFAULT_ADMIN_ID: None}
    env_admin = os.environ.get("ADMIN_ID")
    if env_admin:
        try:
            allowed[int(env_admin)] = None
        except ValueError:
            pass

    db = get_mongo_db()
    loaded_from_mongo = False

    if db is not None:
        try:
            users_coll = db["users"]
            docs = list(users_coll.find({}))
            for doc in docs:
                uid_raw = doc.get("_id") or doc.get("user_id")
                if uid_raw is None:
                    continue
                try:
                    uid = int(uid_raw)
                    exp_val = doc.get("expiry")
                    if exp_val is None:
                        allowed[uid] = None
                    else:
                        if isinstance(exp_val, datetime):
                            dt = exp_val
                        else:
                            dt = datetime.fromisoformat(str(exp_val))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        allowed[uid] = dt
                except Exception as ex:
                    logger.warning(f"Error parsing user doc from Mongo ({doc}): {ex}")
            loaded_from_mongo = True
            logger.info(f"Loaded {len(allowed)} authorized users from MongoDB collection 'users'.")
        except Exception as e:
            logger.warning(f"Failed to fetch users from MongoDB: {e}. Falling back to local JSON.")

    if not loaded_from_mongo and os.path.exists(ALLOWED_USERS_FILE):
        try:
            with open(ALLOWED_USERS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    for uid_str, exp_str in data.items():
                        try:
                            uid = int(uid_str)
                            if exp_str is None:
                                allowed[uid] = None
                            else:
                                dt = datetime.fromisoformat(exp_str)
                                if dt.tzinfo is None:
                                    dt = dt.replace(tzinfo=timezone.utc)
                                allowed[uid] = dt
                        except Exception:
                            pass
                elif isinstance(data, list):
                    for u in data:
                        allowed[int(u)] = None
            logger.info(f"Loaded {len(allowed)} authorized users from local file '{ALLOWED_USERS_FILE}'.")
        except Exception as e:
            logger.warning(f"Could not load allowed_users.json: {e}")

    # Primary Admin is always permanent
    allowed[DEFAULT_ADMIN_ID] = None
    return allowed


def save_allowed_users(allowed: Dict[int, Optional[datetime]]):
    try:
        data = {}
        for uid, exp in allowed.items():
            data[str(uid)] = exp.isoformat() if exp else None
        with open(ALLOWED_USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not save allowed_users.json: {e}")


def persist_user_allow(user_id: int, expiry: Optional[datetime]):
    """Persists granted access for user_id to MongoDB (if available) and local JSON fallback."""
    user_id = int(user_id)
    ALLOWED_USERS[user_id] = expiry

    db = get_mongo_db()
    if db is not None:
        try:
            users_coll = db["users"]
            doc = {
                "_id": user_id,
                "user_id": user_id,
                "expiry": expiry.isoformat() if expiry else None,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            users_coll.replace_one({"_id": user_id}, doc, upsert=True)
            logger.info(f"Persisted user {user_id} allow state to MongoDB.")
        except Exception as e:
            logger.warning(f"Failed to persist user {user_id} allow to MongoDB: {e}")

    save_allowed_users(ALLOWED_USERS)


def persist_user_revoke(user_id: int):
    """Persists revoked access for user_id to MongoDB (if available) and local JSON fallback."""
    user_id = int(user_id)
    if user_id in ALLOWED_USERS:
        del ALLOWED_USERS[user_id]

    db = get_mongo_db()
    if db is not None:
        try:
            users_coll = db["users"]
            users_coll.delete_one({"_id": user_id})
            logger.info(f"Persisted user {user_id} revoke state to MongoDB.")
        except Exception as e:
            logger.warning(f"Failed to persist user {user_id} revoke to MongoDB: {e}")

    save_allowed_users(ALLOWED_USERS)


ALLOWED_USERS: Dict[int, Optional[datetime]] = load_allowed_users()


def save_active_jobs(active_tracks: Dict[Any, Dict[str, Any]]):
    """Saves ACTIVE_TRACKS dictionary to MongoDB collection 'active_jobs' and active_jobs.json."""
    try:
        data = {}
        for key, track in active_tracks.items():
            clean_item = {}
            for k, v in track.items():
                if isinstance(v, (str, int, float, bool, type(None))):
                    clean_item[k] = v
            data[str(key)] = clean_item

        db = get_mongo_db()
        if db is not None:
            try:
                jobs_coll = db["active_jobs"]
                jobs_coll.delete_many({})
                if data:
                    docs = [{"_id": str(k), **v} for k, v in data.items()]
                    jobs_coll.insert_many(docs)
                logger.info(f"Saved {len(data)} active tracking jobs to MongoDB collection 'active_jobs'")
            except Exception as e:
                logger.warning(f"Failed to save active jobs to MongoDB: {e}")

        with open(ACTIVE_JOBS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not save active_jobs.json: {e}")


def load_active_jobs() -> Dict[Any, Dict[str, Any]]:
    """Loads saved tracking jobs from MongoDB or active_jobs.json on startup."""
    db = get_mongo_db()
    if db is not None:
        try:
            jobs_coll = db["active_jobs"]
            docs = list(jobs_coll.find({}))
            if docs:
                loaded_tracks = {}
                for doc in docs:
                    key_str = doc.get("_id")
                    try:
                        key = int(key_str)
                    except (ValueError, TypeError):
                        key = key_str
                    doc_copy = dict(doc)
                    doc_copy.pop("_id", None)
                    doc_copy["chat_id"] = key
                    loaded_tracks[key] = doc_copy
                logger.info(f"Loaded {len(loaded_tracks)} active tracking sessions from MongoDB collection 'active_jobs'")
                return loaded_tracks
        except Exception as e:
            logger.warning(f"Failed to load active jobs from MongoDB: {e}. Falling back to local JSON.")

    target_file = ACTIVE_JOBS_FILE if os.path.exists(ACTIVE_JOBS_FILE) else ("active_job.json" if os.path.exists("active_job.json") else None)
    if not target_file:
        return {}
    try:
        with open(target_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                loaded_tracks = {}
                for key_str, track in data.items():
                    if isinstance(track, dict):
                        try:
                            key = int(key_str)
                        except ValueError:
                            key = key_str
                        track["chat_id"] = key
                        loaded_tracks[key] = track
                logger.info(f"Loaded {len(loaded_tracks)} active tracking sessions from {target_file}")
                return loaded_tracks
    except Exception as e:
        logger.warning(f"Could not load active tracking jobs: {e}")
    return {}


def parse_duration(duration_str: str) -> Optional[timedelta]:
    s = duration_str.strip().lower()
    if not s:
        return None
    m = re.match(r"^(\d+)([dhm]?)$", s)
    if not m:
        return None
    val = int(m.group(1))
    unit = m.group(2)
    if unit == "h":
        return timedelta(hours=val)
    elif unit == "m":
        return timedelta(minutes=val)
    else:  # 'd' or empty default to days
        return timedelta(days=val)


class TelegramOddsBot:
    def __init__(self, token: str):
        self.client = TelegramBotClient(token)
        self.engine = MultiTrackOddsEngine(
            bot_token=token,
            on_odds_update=self._on_odds_update,
            on_alert_trigger=self._on_alert_trigger,
            on_error=self._on_error
        )
        self.last_update_id = 0
        self.running = False
        
        # Bind PORT for Render Web Service health checks (default 10000)
        self.port = int(os.environ.get("PORT", "10000"))
        self.health_server = None
        
        # Shared active tracking state dictionary for instant 0ms /status response
        self.active_tracks: Dict[str, Dict[str, Any]] = {}
        self.tracking_tasks: Dict[str, asyncio.Task] = {}

    def verify_access(self, user_id: int) -> Tuple[bool, bool]:
        """
        Returns (is_allowed, is_expired).
        If access is expired, automatically blocks user and saves allowed_users.json.
        """
        if user_id not in ALLOWED_USERS:
            return False, False

        expiry = ALLOWED_USERS[user_id]
        if expiry is None:
            return True, False  # Permanent access (Admin)

        now = datetime.now(timezone.utc)
        if now > expiry:
            persist_user_revoke(user_id)
            logger.info(f"User {user_id} access expired and blocked.")
            return False, True

        return True, False

    async def start_async(self):
        # 1. Start background HTTP server for Render Web Service Port Scan & Health Checks
        self.health_server = start_health_server(self.port)

        # 2. Verify token
        ok, bot_info = await asyncio.to_thread(self.client.get_me)
        if not ok:
            logger.warning(f"Telegram API Token Verification Warning: {bot_info}. Health server remains active on 0.0.0.0:{self.port}.")
            print(f"\n⚠️ Health Check HTTP Server listening on 0.0.0.0:{self.port}. (Waiting for valid TELEGRAM_BOT_TOKEN)")
        else:
            bot_name = bot_info.get("username", "OddsBot")
            logger.info(f"Bot connected successfully as @{bot_name}")
            print(f"\n==================================================")
            print(f"🚀 Telegram Live Cricket Alert & Cashout Bot (@{bot_name})")
            print(f"Async Architecture: Detached Background Workers (0ms /status)")
            print(f"Status: RUNNING 24/7 (Ball-by-ball Live Exchange Odds)")
            print(f"Admin ID: {DEFAULT_ADMIN_ID} | Authorized Users: {len(ALLOWED_USERS)}")
            print(f"Health Check HTTP Server: 0.0.0.0:{self.port}")
            print(f"==================================================\n")

        # 3. Start Odds Engine async loop
        await self.engine.start_async()
        self.running = True

        # 4. Reload saved active tracking sessions from active_job.json
        saved_tracks = load_active_jobs()
        if saved_tracks:
            ACTIVE_TRACKS.update(saved_tracks)
            for chat_id, track in saved_tracks.items():
                team_name = track.get("team") or track.get("team_name")
                threshold = track.get("target") or track.get("target_odd", 1.18)
                stake_val = track.get("stake", 1000.0)
                entry_odd = track.get("entry") or track.get("entry_odd")

                self.engine.add_track(
                    chat_id=chat_id,
                    match_url="exchange",
                    team_name=team_name,
                    threshold=threshold,
                    operator="<=",
                    poll_interval=2.5,
                    stake=stake_val,
                    entry_odd=entry_odd or 1.01
                )

                task_key = str(chat_id)
                if task_key not in self.tracking_tasks or self.tracking_tasks[task_key].done():
                    logger.info(f"Resuming persisted tracking session for chat {chat_id} ({team_name} <= {threshold})")
                    self.tracking_tasks[task_key] = asyncio.create_task(self.run_monitor(chat_id))

        # 5. Start Telegram Updates Async Polling loop
        try:
            while self.running:
                ok, updates = await asyncio.to_thread(self.client.get_updates, offset=self.last_update_id + 1, timeout=2)
                if ok and updates:
                    for update in updates:
                        self.last_update_id = update["update_id"]
                        self._handle_update(update)
                await asyncio.sleep(0.05)
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("Stopping bot...")
            await self.stop_async()

    def start(self):
        asyncio.run(self.start_async())

    async def stop_async(self):
        self.running = False
        for task in self.tracking_tasks.values():
            task.cancel()
        await self.engine.stop_async()
        await global_exchange_scraper.close_async()
        if self.health_server:
            try:
                self.health_server.shutdown()
            except Exception:
                pass
        logger.info("Bot stopped cleanly.")

    def stop(self):
        asyncio.run(self.stop_async())

    def _handle_update(self, update: Dict[str, Any]):
        callback_query = update.get("callback_query")
        if callback_query:
            cb_id = callback_query.get("id")
            cb_data = callback_query.get("data", "")
            msg = callback_query.get("message", {})
            chat_id = msg.get("chat", {}).get("id")
            from_user = callback_query.get("from", {})
            raw_user_id = from_user.get("id", chat_id)
            try:
                user_id = int(raw_user_id)
            except (ValueError, TypeError):
                user_id = raw_user_id

            if cb_data.startswith("stop_siren:") or cb_data in ["stop_alarm", "mute_alarm", "stop_tracking"]:
                if cb_id:
                    asyncio.create_task(asyncio.to_thread(
                        self.client.answer_callback_query,
                        cb_id,
                        text="🛑 Siren Alarm Stopped!",
                        show_alert=True
                    ))
                if chat_id:
                    asyncio.create_task(self._cmd_stop_async(chat_id, []))
                return

            if cb_data.startswith("match:"):
                try:
                    match_idx = int(cb_data.split(":")[1])
                    asyncio.create_task(self._handle_match_click_async(chat_id, user_id, match_idx, cb_id))
                except (IndexError, ValueError) as e:
                    logger.warning(f"Invalid match callback data ({cb_data}): {e}")
                return

            if cb_data.startswith("select_team:"):
                try:
                    parts = cb_data.split(":")
                    match_idx = int(parts[1])
                    team_idx = int(parts[2])
                    asyncio.create_task(self._handle_team_click_async(chat_id, user_id, match_idx, team_idx, cb_id))
                except (IndexError, ValueError) as e:
                    logger.warning(f"Invalid select_team callback data ({cb_data}): {e}")
                return

            return

        message = update.get("message")
        if not message:
            return

        chat_id = message["chat"]["id"]
        from_user = message.get("from", {})
        raw_user_id = from_user.get("id", chat_id)
        try:
            user_id = int(raw_user_id)
        except (ValueError, TypeError):
            user_id = raw_user_id

        is_admin = (user_id == DEFAULT_ADMIN_ID or chat_id == DEFAULT_ADMIN_ID)

        text = message.get("text", "").strip() if "text" in message else ""
        caption = message.get("caption", "").strip() if "caption" in message else ""
        photo = message.get("photo")

        is_cmd = text.startswith("/")

        if is_cmd:
            parts = text.split()
            cmd = parts[0].lower()

            logger.info(f"Command from chat {chat_id} (user {user_id}): {text}")

            # 1. Admin Commands
            if cmd == "/seturl":
                asyncio.create_task(self._cmd_seturl_async(chat_id, user_id, parts[1:], from_user))
                return

            if cmd in ["/allow", "/revoke", "/users", "/reject", "/msg"]:
                if not is_admin:
                    asyncio.create_task(asyncio.to_thread(self.client.send_message, chat_id, "❌ Only the Admin can use this command.", "HTML", False))
                    return
                if cmd == "/allow":
                    asyncio.create_task(self._cmd_allow_async(chat_id, parts[1:]))
                elif cmd == "/revoke":
                    asyncio.create_task(self._cmd_revoke_async(chat_id, parts[1:]))
                elif cmd == "/users":
                    asyncio.create_task(self._cmd_users_async(chat_id))
                elif cmd == "/reject":
                    asyncio.create_task(self._cmd_reject_async(chat_id, parts[1:]))
                elif cmd == "/msg":
                    asyncio.create_task(self._cmd_msg_async(chat_id, parts[1:]))
                return

            # 2. Public /buy & /feedback commands
            if cmd == "/buy":
                asyncio.create_task(self._cmd_buy_async(chat_id))
                return

            if cmd == "/feedback":
                asyncio.create_task(self._cmd_feedback_async(chat_id, user_id, from_user, parts[1:]))
                return

            # 3. Access & Timed Expiry Verification for all feature commands
            is_allowed, is_expired = self.verify_access(user_id)

            if is_expired:
                asyncio.create_task(asyncio.to_thread(
                    self.client.send_message,
                    chat_id,
                    "⚠️ Your access has expired. Please renew for ₹50/month via /buy.",
                    "HTML", False
                ))
                return

            if not is_allowed:
                if cmd in ["/start", "/help"]:
                    self._handle_unauthorized_start(chat_id, user_id, from_user)
                else:
                    asyncio.create_task(asyncio.to_thread(
                        self.client.send_message,
                        chat_id,
                        "🔒 <b>ACCESS REQUIRED</b>\n\n"
                        "You do not have active authorization.\n"
                        "Send <code>/start</code> to request a 3-day free trial or <code>/buy</code> for subscription details.",
                        "HTML", False
                    ))
                return

            # 4. Dispatch feature commands for authorized users
            if cmd in ["/start", "/help"]:
                self._cmd_start(chat_id, user_id)
            elif cmd == "/matches":
                asyncio.create_task(self._cmd_matches_async(chat_id, user_id))
            elif cmd == "/track":
                asyncio.create_task(asyncio.to_thread(
                    self.client.send_message,
                    chat_id,
                    "ℹ️ Manual <code>/track</code> is deprecated.\nPlease use <code>/matches</code> to select a live match, choose a team, and set your target odd &amp; stake interactively.",
                    "HTML", False
                ))
            elif cmd in ["/status", "/myalert"]:
                asyncio.create_task(self._cmd_status_async(chat_id))
            elif cmd in ["/stop", "/untrack"]:
                asyncio.create_task(self._cmd_stop_async(chat_id, parts[1:]))
            elif cmd == "/mute":
                asyncio.create_task(self._cmd_mute_async(chat_id, parts[1:], muted=True))
            elif cmd == "/unmute":
                asyncio.create_task(self._cmd_mute_async(chat_id, parts[1:], muted=False))
            elif cmd == "/setodd":
                asyncio.create_task(self._cmd_setodd_async(chat_id, parts[1:]))
            else:
                asyncio.create_task(asyncio.to_thread(
                    self.client.send_message,
                    chat_id,
                    "❓ Unknown command. Send <code>/help</code> or <code>/matches</code> to get started.",
                    "HTML", False
                ))
        else:
            # Handle non-command messages (State machine responses or Screenshot/Photo proof submissions)
            if user_id in USER_STATES and USER_STATES[user_id].get("state") in ["AWAITING_TARGET", "AWAITING_STAKE"]:
                asyncio.create_task(self._handle_state_input_async(chat_id, user_id, text))
                return
            if not is_admin:
                asyncio.create_task(self._handle_user_proof_submission(chat_id, user_id, from_user, text, photo, caption))

    def _handle_unauthorized_start(self, chat_id: str | int, user_id: int, from_user: Dict[str, Any]):
        first_name = html.escape(str(from_user.get("first_name", "")))
        last_name = html.escape(str(from_user.get("last_name", "")))
        full_name = f"{first_name} {last_name}".strip() or "User"
        uname = from_user.get("username")
        username = f"@{html.escape(str(uname))}" if uname else "No username"

        user_msg = (
            "🤖 <b>Cricket Live Odds Alert Bot</b>\n\n"
            "Track real-time ball-to-ball cricket market bhav and get instant continuous siren alerts.\n\n"
            "📌 <b>Available Commands:</b>\n"
            "• <code>/matches</code> - View live in-play matches &amp; set new odds alert\n"
            "• <code>/status</code> - Check current live bhav &amp; tracking progress\n"
            "• <code>/untrack</code> - Stop &amp; cancel the current active alert\n"
            "• <code>/help</code> - Show this guide\n\n"
            "⚡ <b>How to use:</b>\n"
            "1. Click <code>/matches</code> to see live games.\n"
            "2. Select your match and team using buttons.\n"
            "3. Enter your target odd &amp; stake.\n"
            "4. Bot will continuously ring siren alerts once your odd hits!\n\n"
            "🎁 <b>Your 3-Day Free Trial Request has been submitted! Admin will activate your access shortly.</b>"
        )
        asyncio.create_task(asyncio.to_thread(self.client.send_message, chat_id, user_msg, "HTML", False))

        admin_card = (
            "🆕 <b>NEW USER TRIAL REQUEST!</b>\n"
            f"👤 <b>Name:</b> {full_name} | <b>Username:</b> {username}\n"
            f"🆔 <b>User ID:</b> <code>{user_id}</code>\n"
            f"👉 <b>Approve:</b> <code>/allow {user_id} 3d</code>"
        )
        asyncio.create_task(asyncio.to_thread(self.client.send_message, DEFAULT_ADMIN_ID, admin_card, "HTML", False))

    async def _cmd_buy_async(self, chat_id: str | int):
        qr_url = "https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=upi://pay?pa=rajdiljeet@fam%26pn=OddsTracker%26am=50%26cu=INR"
        caption = (
            "💳 <b>Subscription Plan: ₹50 / 30 Days</b>\n"
            "UPI ID: <code>rajdiljeet@fam</code> (tap to copy)\n\n"
            "Scan the QR or copy the UPI ID to pay ₹50.\n\n"
            "⚠️ <b>IMPORTANT:</b> Screenshot lene se pehle UPI app me 'View more details' par tap karein aur apna 12-digit UTR / UPI Ref Number screenshot ke sath mandatory submit karein. Bina UTR payment approve nahi hoga."
        )

        ok, _ = await asyncio.to_thread(self.client.send_photo, chat_id, qr_url, caption, "HTML")
        if not ok:
            fallback_msg = f"💳 <b>BUY SUBSCRIPTION</b>\n\n{caption}"
            await asyncio.to_thread(self.client.send_message, chat_id, fallback_msg, "HTML", False)

    async def _handle_user_proof_submission(
        self,
        chat_id: str | int,
        user_id: int,
        from_user: Dict[str, Any],
        text: str,
        photo: Optional[List[Dict[str, Any]]],
        caption: str
    ):
        first_name = html.escape(str(from_user.get("first_name", "")))
        last_name = html.escape(str(from_user.get("last_name", "")))
        full_name = f"{first_name} {last_name}".strip() or "User"
        uname = from_user.get("username")
        username = f"@{html.escape(str(uname))}" if uname else "No username"

        if photo:
            photo_file_id = photo[-1].get("file_id")
            admin_caption = (
                f"💳 <b>PAYMENT PROOF SUBMITTED (SCREENSHOT)!</b>\n\n"
                f"👤 <b>Name:</b> {full_name}\n"
                f"🏷️ <b>Username:</b> {username}\n"
                f"🆔 <b>User ID:</b> <code>{user_id}</code>\n"
                f"💬 <b>Caption:</b> {html.escape(caption) if caption else 'None'}\n\n"
                f"👉 Activate 30-Day Subscription:\n"
                f"<code>/allow {user_id} 30d</code>"
            )
            await asyncio.to_thread(self.client.send_photo, DEFAULT_ADMIN_ID, photo_file_id, admin_caption, "HTML")
        else:
            admin_msg = (
                f"💳 <b>PAYMENT PROOF / MESSAGE SUBMITTED!</b>\n\n"
                f"👤 <b>Name:</b> {full_name}\n"
                f"🏷️ <b>Username:</b> {username}\n"
                f"🆔 <b>User ID:</b> <code>{user_id}</code>\n"
                f"💬 <b>Message:</b> {html.escape(text)}\n\n"
                f"👉 Activate 30-Day Subscription:\n"
                f"<code>/allow {user_id} 30d</code>"
            )
            await asyncio.to_thread(self.client.send_message, DEFAULT_ADMIN_ID, admin_msg, "HTML", False)

        reply_msg = "✅ Received! Admin will verify and activate your access shortly."
        await asyncio.to_thread(self.client.send_message, chat_id, reply_msg, "HTML", False)

    async def _cmd_allow_async(self, chat_id: str | int, args: list):
        if not args:
            await asyncio.to_thread(
                self.client.send_message,
                chat_id,
                "⚠️ <b>Usage Syntax:</b> <code>/allow &lt;user_id&gt; &lt;duration&gt;</code>\n"
                "<i>Example:</i> <code>/allow 12345678 3d</code> (3-day trial)\n"
                "<i>Example:</i> <code>/allow 12345678 30d</code> (30-day paid)",
                "HTML", False
            )
            return

        try:
            target_user_id = int(args[0].strip())
            duration_str = args[1].strip() if len(args) > 1 else "30d"
            delta = parse_duration(duration_str)

            if not delta:
                await asyncio.to_thread(
                    self.client.send_message,
                    chat_id,
                    "❌ Invalid duration format. Use e.g. <code>3d</code>, <code>30d</code>, <code>12h</code>.",
                    "HTML", False
                )
                return

            expiry_dt = datetime.now(timezone.utc) + delta
            persist_user_allow(target_user_id, expiry_dt)

            expiry_str = expiry_dt.strftime("%Y-%m-%d %H:%M UTC")
            admin_msg = f"✅ User <code>{target_user_id}</code> granted access for <b>{duration_str}</b>!\n📅 Expiry: <code>{expiry_str}</code>"
            await asyncio.to_thread(self.client.send_message, chat_id, admin_msg, "HTML", False)

            user_msg = f"🎉 Your access has been activated for {duration_str}!"
            asyncio.create_task(asyncio.to_thread(self.client.send_message, target_user_id, user_msg, "HTML", False))
        except ValueError:
            await asyncio.to_thread(self.client.send_message, chat_id, "❌ Invalid User ID. Must be a numeric Telegram ID.", "HTML", False)

    async def _cmd_revoke_async(self, chat_id: str | int, args: list):
        if not args:
            await asyncio.to_thread(
                self.client.send_message,
                chat_id,
                "⚠️ <b>Usage Syntax:</b> <code>/revoke &lt;user_id&gt;</code>",
                "HTML", False
            )
            return

        try:
            target_user_id = int(args[0].strip())
            if target_user_id == DEFAULT_ADMIN_ID:
                await asyncio.to_thread(self.client.send_message, chat_id, "❌ Cannot revoke primary Admin access.", "HTML", False)
                return

            if target_user_id in ALLOWED_USERS:
                persist_user_revoke(target_user_id)
                admin_msg = f"✅ Access for user <code>{target_user_id}</code> has been revoked."
            else:
                admin_msg = f"ℹ️ User <code>{target_user_id}</code> was not in allowed list."

            await asyncio.to_thread(self.client.send_message, chat_id, admin_msg, "HTML", False)
        except ValueError:
            await asyncio.to_thread(self.client.send_message, chat_id, "❌ Invalid User ID.", "HTML", False)

    async def _cmd_users_async(self, chat_id: str | int):
        user_lines = []
        now = datetime.now(timezone.utc)
        for uid, expiry in sorted(ALLOWED_USERS.items(), key=lambda x: str(x[0])):
            if uid == DEFAULT_ADMIN_ID or expiry is None:
                user_lines.append(f"• <code>{uid}</code> — 👑 Admin (Permanent)")
            else:
                if now > expiry:
                    status_str = "Expired"
                else:
                    rem = expiry - now
                    days = rem.days
                    hrs = rem.seconds // 3600
                    status_str = f"{days}d {hrs}h remaining (Expires: {expiry.strftime('%Y-%m-%d %H:%M UTC')})"
                user_lines.append(f"• <code>{uid}</code> — {status_str}")

        msg = (
            f"👥 <b>AUTHORIZED USERS LIST</b>\n\n"
            f"👑 <b>Admin ID:</b> <code>{DEFAULT_ADMIN_ID}</code>\n"
            f"📊 <b>Total Users:</b> {len(ALLOWED_USERS)}\n\n"
            + "\n".join(user_lines)
        )
        await asyncio.to_thread(self.client.send_message, chat_id, msg, "HTML", False)

    async def _cmd_reject_async(self, chat_id: str | int, args: list):
        if not args:
            await asyncio.to_thread(
                self.client.send_message,
                chat_id,
                "⚠️ <b>Usage Syntax:</b> <code>/reject &lt;user_id&gt;</code>",
                "HTML", False
            )
            return

        try:
            target_user_id = int(args[0].strip())
            reject_msg = "❌ Payment verification failed or funds not received. Please verify your UTR and contact support."
            ok, info = await asyncio.to_thread(self.client.send_message, target_user_id, reject_msg, "HTML", False)
            if ok:
                await asyncio.to_thread(self.client.send_message, chat_id, f"✅ Rejection alert sent to user <code>{target_user_id}</code>.", "HTML", False)
            else:
                await asyncio.to_thread(self.client.send_message, chat_id, f"⚠️ Failed to send rejection to <code>{target_user_id}</code>: {info}", "HTML", False)
        except ValueError:
            await asyncio.to_thread(self.client.send_message, chat_id, "❌ Invalid User ID.", "HTML", False)

    async def _cmd_msg_async(self, chat_id: str | int, args: list):
        if len(args) < 2:
            await asyncio.to_thread(
                self.client.send_message,
                chat_id,
                "⚠️ <b>Usage Syntax:</b> <code>/msg &lt;user_id&gt; &lt;custom_text&gt;</code>",
                "HTML", False
            )
            return

        try:
            target_user_id = int(args[0].strip())
            custom_text = " ".join(args[1:]).strip()
            ok, info = await asyncio.to_thread(self.client.send_message, target_user_id, custom_text, "HTML", False)
            if ok:
                await asyncio.to_thread(self.client.send_message, chat_id, f"✅ Message sent to user <code>{target_user_id}</code>.", "HTML", False)
            else:
                await asyncio.to_thread(self.client.send_message, chat_id, f"⚠️ Failed to send message to <code>{target_user_id}</code>: {info}", "HTML", False)
        except ValueError:
            await asyncio.to_thread(self.client.send_message, chat_id, "❌ Invalid User ID.", "HTML", False)

    async def _cmd_feedback_async(self, chat_id: str | int, user_id: int, from_user: Dict[str, Any], args: list):
        feedback_text = " ".join(args).strip()
        if not feedback_text:
            await asyncio.to_thread(
                self.client.send_message,
                chat_id,
                "⚠️ <b>Usage Syntax:</b> <code>/feedback &lt;your message&gt;</code>",
                "HTML", False
            )
            return

        first_name = html.escape(str(from_user.get("first_name", "")))
        last_name = html.escape(str(from_user.get("last_name", "")))
        full_name = f"{first_name} {last_name}".strip() or "User"
        uname = from_user.get("username")
        username = f"@{html.escape(str(uname))}" if uname else "No username"

        admin_card = (
            f"💬 <b>NEW USER FEEDBACK:</b>\n"
            f"From: {full_name} ({username} | <code>{user_id}</code>)\n"
            f"Message: {html.escape(feedback_text)}\n\n"
            f"👉 Direct Reply: <code>/msg {user_id} Your reply here</code>"
        )
        asyncio.create_task(asyncio.to_thread(self.client.send_message, DEFAULT_ADMIN_ID, admin_card, "HTML", False))

        reply_msg = "✅ Feedback sent to admin. Thank you!"
        await asyncio.to_thread(self.client.send_message, chat_id, reply_msg, "HTML", False)

    async def _cmd_seturl_async(self, chat_id: str | int, user_id: int, args: list, from_user: Dict[str, Any]):
        sender_id = from_user.get("id") if from_user else user_id
        try:
            sender_id = int(sender_id)
        except (ValueError, TypeError):
            pass

        if sender_id != ADMIN_ID:
            await asyncio.to_thread(self.client.send_message, chat_id, "Unauthorized", "HTML", False)
            return

        if not args:
            await asyncio.to_thread(
                self.client.send_message,
                chat_id,
                "⚠️ <b>Usage Syntax:</b> <code>/seturl &lt;new_url&gt;</code>\n"
                "<i>Example:</i> <code>/seturl https://crex.live</code>",
                "HTML", False
            )
            return

        raw_url = args[0].strip()
        target_url = raw_url if raw_url.startswith(("http://", "https://")) else f"https://{raw_url}"
        parsed = urllib.parse.urlparse(target_url)
        if not parsed.netloc:
            await asyncio.to_thread(
                self.client.send_message,
                chat_id,
                "❌ Invalid URL structure. Please provide a valid domain (e.g., <code>https://crex.live</code>).",
                "HTML", False
            )
            return

        updated_url = set_base_url(target_url)

        status_str = "OK"
        try:
            session = await global_exchange_scraper.get_aiohttp_session()
            async with session.get(updated_url, timeout=aiohttp.ClientTimeout(total=5.0)) as resp:
                status_str = f"200 OK" if resp.status == 200 else f"{resp.status}"
        except Exception:
            try:
                req = urllib.request.Request(updated_url, headers=global_exchange_scraper.http_headers)
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    status_str = f"{resp.getcode()} OK"
            except Exception:
                status_str = "OK"

        reply_msg = f"✅ Base domain updated to: {updated_url}\nStatus: {status_str}"
        await asyncio.to_thread(self.client.send_message, chat_id, reply_msg, "HTML", False)

    def _cmd_start(self, chat_id: str | int, user_id: Optional[str | int] = None):
        is_admin = (user_id and (user_id == DEFAULT_ADMIN_ID or str(user_id) == str(DEFAULT_ADMIN_ID)))
        admin_extra = (
            "\n\n👑 <b>Admin Commands:</b>\n"
            "• <code>/allow &lt;user_id&gt; &lt;duration&gt;</code> — Grant access (e.g. <code>/allow 12345678 30d</code>)\n"
            "• <code>/revoke &lt;user_id&gt;</code> — Revoke user authorization\n"
            "• <code>/reject &lt;user_id&gt;</code> — Reject user payment\n"
            "• <code>/msg &lt;user_id&gt; &lt;text&gt;</code> — Direct message user\n"
            "• <code>/users</code> — View all active users &amp; remaining days\n"
            "• <code>/setodd &lt;team&gt; &lt;odd&gt;</code> — Modify live odd for instant testing"
        ) if is_admin else ""

        help_text = (
            "🤖 <b>Cricket Live Odds Alert Bot</b>\n\n"
            "Track real-time ball-to-ball cricket market bhav and get instant continuous siren alerts.\n\n"
            "📌 <b>Available Commands:</b>\n"
            "• <code>/matches</code> - View live in-play matches &amp; set new odds alert\n"
            "• <code>/status</code> - Check current live bhav &amp; tracking progress\n"
            "• <code>/untrack</code> - Stop &amp; cancel the current active alert\n"
            "• <code>/help</code> - Show this guide\n\n"
            "⚡ <b>How to use:</b>\n"
            "1. Click <code>/matches</code> to see live games.\n"
            "2. Select your match and team using buttons.\n"
            "3. Enter your target odd &amp; stake.\n"
            "4. Bot will continuously ring siren alerts once your odd hits!"
            f"{admin_extra}"
        )
        self.client.send_message(chat_id, help_text)

    async def _cmd_matches_async(self, chat_id: str | int, user_id: Optional[int] = None):
        try:
            raw_matches = await global_exchange_scraper.fetch_live_matches_async()
            last_status = getattr(global_exchange_scraper, "last_fetch_status", 200)
        except Exception as e:
            logger.error(f"❌ Explicit CREX Live Fetch Error in /matches: {e}")
            raw_matches = []
            last_status = 500

        matches = []
        for m in raw_matches:
            home = m.get("home_team", "").strip()
            away = m.get("away_team", "").strip()
            if not home or not away or home.lower() == away.lower():
                continue
            odds = m.get("odds", [])
            if odds and any(o.get("back", 0) > 1.0 for o in odds):
                matches.append(m)

        if not matches:
            logger.warning(f"❌ CREX Live Query Notice: /matches retrieved 0 live in-play matches (Status: {last_status}, Raw count: {len(raw_matches)}).")
            if last_status in [403, 429, 503]:
                msg = (
                    "⚠️ <b>Live CREX matches auto-fetch blocked by firewall.</b>\n\n"
                    "Please copy the match link directly from CREX and use <code>/seturl &lt;match_link&gt;</code> to track live ball-to-ball rates."
                )
            else:
                msg = "🏏 No live in-play matches on CREX right now."
            await asyncio.to_thread(self.client.send_message, chat_id, msg, "HTML", False)
            return

        uid = user_id if user_id is not None else (int(chat_id) if str(chat_id).lstrip("-").isdigit() else chat_id)
        USER_STATES[uid] = {
            "state": "MATCH_SELECT",
            "matches": matches
        }

    async def _cmd_matches_async(self, chat_id: str | int, user_id: Optional[int] = None):
        try:
            raw_matches = await global_exchange_scraper.fetch_live_matches_async()
            last_status = getattr(global_exchange_scraper, "last_fetch_status", 200)
        except Exception as e:
            logger.error(f"❌ Explicit CREX Live Fetch Error in /matches: {e}")
            raw_matches = []
            last_status = 500

        matches = []
        for m in raw_matches:
            home = m.get("home_team", "").strip()
            away = m.get("away_team", "").strip()
            if not home or not away or home.lower() == away.lower():
                continue
            odds = m.get("odds", [])
            if odds and any(o.get("back", 0) > 1.0 for o in odds):
                matches.append(m)

        if not matches:
            logger.warning(f"❌ CREX Live Query Notice: /matches retrieved 0 live in-play matches (Status: {last_status}, Raw count: {len(raw_matches)}).")
            if last_status in [403, 429, 503]:
                msg = (
                    "⚠️ <b>Live CREX matches auto-fetch blocked by firewall.</b>\n\n"
                    "Please copy the match link directly from CREX and use <code>/seturl &lt;match_link&gt;</code> to track live ball-to-ball rates."
                )
            else:
                msg = "🏏 No live in-play matches on CREX right now."
            await asyncio.to_thread(self.client.send_message, chat_id, msg, "HTML", False)
            return

        uid = user_id if user_id is not None else (int(chat_id) if str(chat_id).lstrip("-").isdigit() else chat_id)
        USER_STATES[uid] = {
            "state": "MATCH_SELECT",
            "matches": matches
        }

        # Step 1: Render compact 2-column grid layout with short team labels (Max 20-24 chars)
        inline_keyboard = []
        row = []
        for idx, m in enumerate(matches):
            home_team = m.get("home_team", "Team 1")
            away_team = m.get("away_team", "Team 2")
            btn_text = format_short_match_button_label(home_team, away_team)
            row.append({"text": btn_text, "callback_data": f"match:{idx}"})
            if len(row) == 2:
                inline_keyboard.append(row)
                row = []
        if row:
            inline_keyboard.append(row)

        reply_markup = {"inline_keyboard": inline_keyboard}
        msg_text = (
            "🏏 <b>LIVE IN-PLAY CRICKET MATCHES</b>\n\n"
            "Tap any live match below to select a team and set target odd:"
        )
        await asyncio.to_thread(self.client.send_message, chat_id, msg_text, "HTML", False, reply_markup)

    async def _handle_match_click_async(self, chat_id: str | int, user_id: int, match_idx: int, cb_id: str):
        """Step 2: Team selection buttons with fresh real-time CREX match re-scrape."""
        if cb_id:
            asyncio.create_task(asyncio.to_thread(self.client.answer_callback_query, cb_id))

        user_state = USER_STATES.get(user_id, {})
        matches = user_state.get("matches", [])

        if not matches or match_idx < 0 or match_idx >= len(matches):
            msg_text = "⚠️ Match session expired. Send /matches to view current live matches."
            await asyncio.to_thread(self.client.send_message, chat_id, msg_text, "HTML", False)
            return

        selected_match = matches[match_idx]
        match_slug = selected_match.get("slug") or selected_match.get("id") or selected_match.get("match_slug")

        # Force fresh live CREX re-scrape on match selection
        if match_slug:
            fresh_match = await global_exchange_scraper.scrape_single_match_by_slug_async(match_slug)
            if fresh_match:
                selected_match = fresh_match
                matches[match_idx] = fresh_match
                USER_STATES[user_id]["matches"] = matches

        home_team = selected_match.get("home_team", "Team 1")
        away_team = selected_match.get("away_team", "Team 2")
        odds_list = selected_match.get("odds", [])

        home_odd = next((o for o in odds_list if is_team_match(o.get("name"), home_team)), odds_list[0] if len(odds_list) > 0 else None)
        away_odd = next((o for o in odds_list if is_team_match(o.get("name"), away_team)), odds_list[1] if len(odds_list) > 1 else None)

        home_back = home_odd.get("back") if home_odd else None
        home_lay = home_odd.get("lay") if home_odd else None
        away_back = away_odd.get("back") if away_odd else None
        away_lay = away_odd.get("lay") if away_odd else None

        home_bhav_str = (home_odd.get("indian_odds") if (home_odd and home_odd.get("indian_odds")) else format_indian_odds(home_back, home_lay)) or "Rate Suspended"
        away_bhav_str = (away_odd.get("indian_odds") if (away_odd and away_odd.get("indian_odds")) else format_indian_odds(away_back, away_lay)) or "Rate Suspended"

        USER_STATES[user_id]["selected_match"] = selected_match

        home_short = clean_short_team_name(home_team)
        away_short = clean_short_team_name(away_team)

        inline_keyboard = [
            [
                {"text": f"🟢 {home_short} (Bhav: {home_bhav_str})", "callback_data": f"select_team:{match_idx}:0"}
            ],
            [
                {"text": f"🔴 {away_short} (Bhav: {away_bhav_str})", "callback_data": f"select_team:{match_idx}:1"}
            ]
        ]

        reply_markup = {"inline_keyboard": inline_keyboard}
        msg_text = f"Select the team to monitor for <b>{home_team} vs {away_team}</b>:"

        await asyncio.to_thread(self.client.send_message, chat_id, msg_text, "HTML", False, reply_markup)

    async def _handle_team_click_async(self, chat_id: str | int, user_id: int, match_idx: int, team_idx: int, cb_id: str):
        """Step 3 Start: Target odd prompt with fresh real-time CREX rate lock."""
        if cb_id:
            asyncio.create_task(asyncio.to_thread(self.client.answer_callback_query, cb_id))
        user_state = USER_STATES.get(user_id, {})
        matches = user_state.get("matches", [])

        if not matches or match_idx < 0 or match_idx >= len(matches):
            msg_text = "⚠️ Match session expired. Send /matches to view current live matches."
            await asyncio.to_thread(self.client.send_message, chat_id, msg_text, "HTML", False)
            return

        selected_match = matches[match_idx]
        home_team = selected_match.get("home_team", "Team 1")
        away_team = selected_match.get("away_team", "Team 2")
        match_slug = selected_match.get("slug") or selected_match.get("id") or selected_match.get("match_slug")

        chosen_team = home_team if team_idx == 0 else away_team
        opp_team = away_team if team_idx == 0 else home_team

        # Fetch fresh odds for selected team
        entry_back = None
        if match_slug:
            live_data = await global_exchange_scraper.get_live_odds_data_for_match_slug_async(match_slug, chosen_team)
            entry_back = live_data.get("target_odd")

        if entry_back is None:
            odds_list = selected_match.get("odds", [])
            chosen_odd_obj = next((o for o in odds_list if is_team_match(o.get("name"), chosen_team)), None)
            entry_back = chosen_odd_obj.get("back") if chosen_odd_obj else None

        live_bhav_str = format_indian_odds(entry_back) if entry_back else "Rate Suspended"

        USER_STATES[user_id] = {
            "state": "AWAITING_TARGET",
            "selected_match": selected_match,
            "chosen_team": chosen_team,
            "opp_team": opp_team,
            "entry_odd": entry_back,
            "live_bhav_str": live_bhav_str
        }

        msg_text = (
            f"Selected: <b>{chosen_team}</b> (Current Bhav: <b>{live_bhav_str}</b>)\n\n"
            f"Reply with your target bhav/odd (e.g., 20 or 1.20):"
        )

        await asyncio.to_thread(self.client.send_message, chat_id, msg_text, "HTML", False)

    async def _handle_state_input_async(self, chat_id: str | int, user_id: int, text: str):
        """Step 3 Continuation: Handle interactive Target & Stake text responses."""
        state_data = USER_STATES.get(user_id)
        if not state_data:
            return

        current_state = state_data.get("state")

        if current_state == "AWAITING_TARGET":
            target_odd = parse_target_odd_input(text)
            if target_odd is None:
                msg = "❌ Invalid target odd. Please enter a valid rate (e.g., <code>20</code> or <code>1.20</code>):"
                await asyncio.to_thread(self.client.send_message, chat_id, msg, "HTML", False)
                return

            state_data["target_odd"] = target_odd
            state_data["state"] = "AWAITING_STAKE"

            msg_text = "Enter your stake amount for cashout/P&L calculation (e.g. 5000) or send '0' to skip:"
            await asyncio.to_thread(self.client.send_message, chat_id, msg_text, "HTML", False)
            return

        elif current_state == "AWAITING_STAKE":
            stake_val = parse_stake_input(text)
            if stake_val is None:
                msg = "❌ Invalid stake amount. Enter a numeric stake (e.g. <code>5000</code>) or <code>0</code>:"
                await asyncio.to_thread(self.client.send_message, chat_id, msg, "HTML", False)
                return

            team_name = state_data.get("chosen_team", "Team")
            target_odd = state_data.get("target_odd", 1.20)
            entry_odd = state_data.get("entry_odd")
            selected_match = state_data.get("selected_match", {})
            opp_team = state_data.get("opp_team")
            match_slug = selected_match.get("slug") or selected_match.get("id")

            clean_team = global_exchange_scraper._clean_team_name(team_name)

            track_entry = {
                "chat_id": chat_id,
                "team": clean_team,
                "team_name": clean_team,
                "target_team_clean": team_name.upper(),
                "match_slug": match_slug,
                "target": target_odd,
                "target_odd": target_odd,
                "entry": entry_odd,
                "entry_odd": entry_odd,
                "current_odd": entry_odd,
                "last_seen_odd": entry_odd,
                "opponent_team": opp_team,
                "stake": stake_val,
                "data_source": "⚡ Live Exchange Feed",
                "start_time": time.time(),
                "muted": False,
                "status": "ACTIVE",
                "operator": "<=",
                "last_alert_time": 0.0,
                "has_triggered": False,
                "frozen_since": None
            }
            ACTIVE_TRACKS[chat_id] = track_entry
            save_active_jobs(ACTIVE_TRACKS)

            self.engine.add_track(
                chat_id=chat_id,
                match_url="exchange",
                team_name=clean_team,
                threshold=target_odd,
                operator="<=",
                poll_interval=2.5,
                stake=stake_val,
                entry_odd=entry_odd or 1.01
            )

            USER_STATES.pop(user_id, None)

            msg_text = (
                f"✅ Tracking active for <b>{team_name}</b> at target <b>{target_odd:.2f}</b> | Stake: ₹{stake_val:,.0f}."
            )
            await asyncio.to_thread(self.client.send_message, chat_id, msg_text, "HTML", False)

            task_key = str(chat_id)
            if task_key in self.tracking_tasks:
                self.tracking_tasks[task_key].cancel()

            self.tracking_tasks[task_key] = asyncio.create_task(self.run_monitor(chat_id))


    async def _cmd_track_async(self, chat_id: str | int, user_id: int, args: list):
        if not args:
            await asyncio.to_thread(
                self.client.send_message,
                chat_id,
                "⚠️ <b>Usage Syntax:</b>\n"
                "<code>/track &lt;team&gt; &lt;target_odd&gt; [stake]</code>\n\n"
                "<i>Example:</i> <code>/track England U19 1.18 1000</code>\n"
                "<i>Example:</i> <code>/track Zimbabwe 2.40 1000</code>",
                "HTML", False
            )
            return

        if args[0].lower() in ["mock", "local", "exchange", "test"]:
            raw_source = args[0]
            params = args[1:]
        else:
            raw_source = "exchange"
            params = args

        if len(params) < 2:
            await asyncio.to_thread(
                self.client.send_message,
                chat_id,
                "⚠️ <b>Usage Syntax:</b>\n"
                "<code>/track &lt;team&gt; &lt;target_odd&gt; [stake]</code>\n\n"
                "<i>Example:</i> <code>/track England U19 1.18 1000</code>\n"
                "<i>Example:</i> <code>/track Zimbabwe 2.40 1000</code>",
                "HTML", False
            )
            return

        def _parse_num(val_str: str) -> Optional[float]:
            try:
                return float(val_str.replace("₹", "").replace("$", ""))
            except (ValueError, TypeError):
                return None

        last_num = _parse_num(params[-1])
        second_last_num = _parse_num(params[-2]) if len(params) >= 3 else None

        stake_val = 1000.0
        threshold = None

        if len(params) >= 3 and second_last_num is not None and last_num is not None:
            threshold = second_last_num
            stake_val = last_num
            team_words = params[:-2]
        elif last_num is not None:
            threshold = last_num
            team_words = params[:-1]
        else:
            await asyncio.to_thread(
                self.client.send_message,
                chat_id,
                "❌ Invalid target odd. Must be a number like <code>2.40</code> or <code>1.18</code>.",
                "HTML", False
            )
            return

        raw_team_name = " ".join(team_words).strip()
        if not raw_team_name:
            await asyncio.to_thread(
                self.client.send_message,
                chat_id,
                "❌ Please specify a valid team name.",
                "HTML", False
            )
            return

        clean_team = global_exchange_scraper._clean_team_name(raw_team_name)

        odds_data = await global_exchange_scraper.get_live_odds_data_for_team_async(clean_team)
        target_display = (odds_data.get("target_team") or clean_team).upper()
        match_slug = odds_data.get("match_slug") or odds_data.get("match_id") or odds_data.get("id")
        entry_odd = odds_data.get("target_odd")
        if entry_odd is not None and not (isinstance(entry_odd, (int, float)) and entry_odd > 1.01):
            entry_odd = None

        if entry_odd is not None and isinstance(entry_odd, (int, float)) and entry_odd > 1.01:
            lay_stake_proj = round((entry_odd * stake_val) / max(0.01, threshold), 2)
            profit_proj = round(lay_stake_proj - stake_val, 2)
            ind_entry = format_indian_odds(entry_odd)
            entry_line = f"📊 <b>Entry Odd ({html.escape(target_display)}):</b> {entry_odd:.2f} (<code>{ind_entry}</code>)\n"
            profit_line = f"💰 <b>Projected Green Book Profit:</b> +₹{profit_proj:,.2f} (Both sides equal profit 💚)\n"
            lay_line = f"📈 <b>Required Lay Stake at Target:</b> Lay <b>₹{lay_stake_proj:,.2f}</b> on <b>{html.escape(target_display)}</b> @ {threshold:.2f}\n\n"
        else:
            entry_line = f"📊 <b>Entry Odd ({html.escape(target_display)}):</b> <i>Fetching live market rate...</i>\n"
            profit_line = f"💰 <b>Projected Green Book Profit:</b> <i>Will calculate on live rate lock 💚</i>\n"
            lay_line = f"📈 <b>Required Lay Stake at Target:</b> Lay on <b>{html.escape(target_display)}</b> @ {threshold:.2f}\n\n"

        track_entry = {
            "chat_id": chat_id,
            "team": clean_team,
            "team_name": clean_team,
            "target_team_clean": target_display,
            "match_slug": match_slug,
            "target": threshold,
            "target_odd": threshold,
            "entry": entry_odd,
            "entry_odd": entry_odd,
            "current_odd": entry_odd,
            "last_seen_odd": entry_odd,
            "opponent_team": odds_data.get("opponent_team"),
            "opponent_odd": odds_data.get("opponent_odd"),
            "stake": stake_val,
            "data_source": "⚡ Live Exchange Feed",
            "start_time": time.time(),
            "muted": False,
            "status": "ACTIVE",
            "operator": "<=",
            "last_alert_time": 0.0,
            "has_triggered": False,
            "frozen_since": None
        }
        ACTIVE_TRACKS[chat_id] = track_entry
        save_active_jobs(ACTIVE_TRACKS)

        # Silent Telemetry Log to Admin for Trial User Activity
        if user_id != DEFAULT_ADMIN_ID:
            telemetry_msg = f"👤 [Trial Activity] User <code>{user_id}</code> started tracking: {html.escape(target_display)} @ target {threshold:.2f}."
            asyncio.create_task(asyncio.to_thread(self.client.send_message, DEFAULT_ADMIN_ID, telemetry_msg, "HTML", True))

        self.engine.add_track(
            chat_id=chat_id,
            match_url=raw_source,
            team_name=clean_team,
            threshold=threshold,
            operator="<=",
            poll_interval=2.5,
            stake=stake_val,
            entry_odd=entry_odd or 1.01
        )

        ind_target = format_indian_odds(threshold)

        opp_team = (odds_data.get("opponent_team") or "").upper()
        opp_odd = odds_data.get("opponent_odd")
        opp_line = ""
        if opp_team and isinstance(opp_odd, (int, float)):
            opp_ind = format_indian_odds(opp_odd)
            opp_line = f"⚔️ <b>Opponent Odd ({html.escape(opp_team)}):</b> {opp_odd:.2f} (<code>{opp_ind}</code>)\n"

        msg = (
            f"🚀 <b>LIVE ODDS TRACKING STARTED!</b>\n\n"
            f"🎯 <b>Target Team:</b> {html.escape(target_display)}\n"
            f"{entry_line}"
            f"{opp_line}"
            f"🎯 <b>Target Odd:</b> {threshold:.2f} (<code>{ind_target}</code>)\n"
            f"💵 <b>Invested Stake:</b> ₹{stake_val:,.0f}\n"
            f"📡 <b>Data Source:</b> ⚡ Live Exchange Feed\n"
            f"{profit_line}"
            f"{lay_line}"
            f"⚡ <i>Live market odds monitored in real time. Send <code>/status</code> anytime for instant updates!</i>"
        )
        await asyncio.to_thread(self.client.send_message, chat_id, msg, "HTML", False)

        task_key = str(chat_id)
        if task_key in self.tracking_tasks:
            self.tracking_tasks[task_key].cancel()

        self.tracking_tasks[task_key] = asyncio.create_task(self.run_monitor(chat_id))

    async def run_monitor(self, chat_id: str | int):
        logger.info(f"Started run_monitor Siren Alarm task for chat {chat_id}")
        last_alert_time = 0.0

        stop_keyboard = {
            "inline_keyboard": [
                [
                    {"text": "🛑 STOP ALARM", "callback_data": "stop_alarm"}
                ]
            ]
        }

        while self.running and (chat_id in ACTIVE_TRACKS or str(chat_id) in ACTIVE_TRACKS):
            try:
                key = chat_id if chat_id in ACTIVE_TRACKS else str(chat_id)
                track = ACTIVE_TRACKS[key]
                team_name = track.get("team") or track.get("team_name")
                match_slug = track.get("match_slug")

                # 1. Fetch live odds strictly locked to match_slug if available
                if match_slug:
                    odds_data = await global_exchange_scraper.get_live_odds_data_for_match_slug_async(match_slug, team_name)
                else:
                    odds_data = await global_exchange_scraper.get_live_odds_data_for_team_async(team_name)

                latest_odd = odds_data.get("target_odd") if odds_data else None

                # 1. Stricter Match End Condition: Conclude ONLY if API/feed data explicitly returns COMPLETED, RESULT, or MATCH_OVER
                is_finished_flag = odds_data.get("is_finished", False) if odds_data else False
                status_str = (odds_data.get("status") or "").upper() if odds_data else ""

                strict_end_keywords = ["COMPLETED", "RESULT", "MATCH_OVER", "MATCH ENDED", "FINISHED", "ABANDONED", "NO RESULT"]
                is_explicitly_concluded = (
                    any(kw in status_str for kw in strict_end_keywords) or
                    (is_finished_flag and status_str not in ["IN-PLAY", "LIVE", "LIVE MATCH"])
                )

                if is_explicitly_concluded:
                    logger.info(f"Match explicitly concluded for chat {chat_id} (status: '{status_str}'). Cleaning up tracking task.")
                    target_val = track.get("target", track.get("target_odd", 0.0))
                    target_display = (track.get("target_team_clean") or team_name).upper()
                    has_triggered = track.get("has_triggered", False)

                    # Hard Auto-Kill Action: Purge session state & remove engine track
                    if key in ACTIVE_TRACKS:
                        del ACTIVE_TRACKS[key]
                    if chat_id in ACTIVE_TRACKS:
                        del ACTIVE_TRACKS[chat_id]
                    if str(chat_id) in ACTIVE_TRACKS:
                        del ACTIVE_TRACKS[str(chat_id)]
                    save_active_jobs(ACTIVE_TRACKS)
                    self.engine.remove_track(chat_id)

                    # Send conclusion notification ONCE
                    if not has_triggered and not track.get("concluded_notified"):
                        track["concluded_notified"] = True
                        winner_name = odds_data.get("winner") if odds_data else None
                        if not winner_name:
                            winner_name = target_display
                        else:
                            winner_name = winner_name.upper()

                        end_msg = (
                            f"🏁 <b>MATCH CONCLUDED!</b>\n"
                            f"🏆 <b>Winner:</b> {html.escape(winner_name)} won the match.\n"
                            f"⚠️ Target odd ({target_val:.2f}) was not reached.\n"
                            f"🛑 Live tracking session has ended and memory cleared."
                        )
                        try:
                            await asyncio.to_thread(self.client.send_message, chat_id, end_msg, "HTML", False)
                        except Exception as e:
                            logger.warning(f"Could not send conclusion message to {chat_id}: {e}")

                    task_key = str(chat_id)
                    if task_key in self.tracking_tasks:
                        task = self.tracking_tasks.pop(task_key, None)
                        if task and not task.done():
                            task.cancel()
                    break

                # 2. Retain state and retry on next tick if odds are temporarily absent or suspended (ball in air, review, network glitch)
                if latest_odd is None or not isinstance(latest_odd, (int, float)) or latest_odd <= 1.01:
                    logger.debug(f"Odds suspended/absent for chat {chat_id}. Retaining previous state and retrying on next tick.")
                    await asyncio.sleep(5.0)
                    continue

                # Live valid odds available (> 1.01)
                track["current_odd"] = latest_odd
                track["last_seen_odd"] = latest_odd
                if odds_data.get("opponent_team"):
                    track["opponent_team"] = odds_data["opponent_team"]
                    track["opponent_odd"] = odds_data["opponent_odd"]
                if odds_data.get("target_team"):
                    track["target_team_clean"] = odds_data["target_team"]
                if odds_data.get("match_slug"):
                    track["match_slug"] = odds_data["match_slug"]
                if track.get("entry") is None or not isinstance(track.get("entry"), (int, float)) or track.get("entry") <= 1.01:
                    track["entry"] = latest_odd
                    track["entry_odd"] = latest_odd

                # 2. Check threshold trigger ONLY if real valid numeric odd (> 1.01) is scraped
                curr = track.get("current_odd")
                target_val = track.get("target", track.get("target_odd", 0.0))
                target_display = (track.get("target_team_clean") or team_name).upper()

                print(f"[LIVE TICK] Tracked: {target_display} | Current: {curr} | Target: {target_val}", flush=True)

                if curr is not None and isinstance(curr, (int, float)) and curr > 1.01 and curr <= target_val:
                    track["status"] = "TRIGGERED"
                    track["has_triggered"] = True
                    now = time.time()

                    # Repeat alert every 8 seconds (Rate-Limit Safety) when not muted
                    if not track.get("muted") and (now - last_alert_time >= 8.0):
                        last_alert_time = now

                        opp_display = (track.get("opponent_team") or "").upper()
                        opp_odd = track.get("opponent_odd")

                        opp_line = ""
                        if opp_display and isinstance(opp_odd, (int, float)):
                            opp_ind = format_indian_odds(opp_odd)
                            opp_line = f"⚔️ <b>Opponent Odd ({html.escape(opp_display)}):</b> {opp_odd:.2f} (<code>{opp_ind}</code>)\n"

                        entry_val = track.get("entry", curr)
                        stake_val = track.get("stake", 1000.0)
                        lay_stake = round((entry_val * stake_val) / max(0.01, curr), 2)
                        profit = round(lay_stake - stake_val, 2)
                        ind_odd = format_indian_odds(curr)

                        alert_msg = (
                            f"🚨 <b>SIREN ALARM MODE: TARGET HIT!</b> 🚨\n\n"
                            f"🎯 <b>Target Team:</b> {html.escape(target_display)}\n"
                            f"📈 <b>Current Live Odd ({html.escape(target_display)}):</b> {curr:.2f} (<code>{ind_odd}</code>) [Target: &lt;= {target_val:.2f}]\n"
                            f"{opp_line}"
                            f"📡 <b>Data Source:</b> ⚡ Live Exchange Feed\n"
                            f"📊 <b>Entry Odd (Auto Locked):</b> {entry_val:.2f}\n"
                            f"💵 <b>Invested Stake:</b> ₹{stake_val:,.0f}\n\n"
                            f"💰 <b>GREEN BOOK CASHOUT BREAKDOWN:</b>\n"
                            f"👉 <b>LAY AMOUNT TO PLACE ON EXCHANGE:</b> Place <b>₹{lay_stake:,.2f} Lay</b> on <b>{html.escape(target_display)}</b> @ {curr:.2f}\n"
                            f"💚 <b>PROJECTED GREEN BOOK PROFIT:</b> <b>+₹{profit:,.2f}</b> (Both sides equal profit)\n\n"
                            f"⚡ <b>ACTION REQUIRED:</b> Place exact Lay amount of <b>₹{lay_stake:,.2f}</b> on <b>{html.escape(target_display)}</b> @ {curr:.2f} to lock profit!\n\n"
                            f"🔔 <i>Siren Alarm repeating every 8 seconds until silenced. Tap button below or send <code>/stop</code> to end.</i>"
                        )
                        await asyncio.to_thread(
                            self.client.send_message,
                            chat_id,
                            alert_msg,
                            "HTML",
                            False,
                            stop_keyboard
                        )
                else:
                    # Rebound detection: if odds bounce back ABOVE target threshold
                    if track.get("status") == "TRIGGERED" and curr is not None and isinstance(curr, (int, float)) and curr > target_val:
                        ind_curr = format_indian_odds(curr)
                        rebound_msg = (
                            f"ℹ️ <b>SIREN ALARM PAUSED — ODDS REBOUNDED ABOVE TARGET</b>\n\n"
                            f"📈 <b>Current Live Odd ({html.escape(target_display)}):</b> <b>{curr:.2f}</b> (<code>{ind_curr}</code>) &gt; Target {target_val:.2f}\n"
                            f"⚡ <i>Live rate bounced back above target. Siren paused, back to passive monitoring.</i>"
                        )
                        await asyncio.to_thread(self.client.send_message, chat_id, rebound_msg, "HTML", False)

                    track["status"] = "ACTIVE"
                    last_alert_time = 0.0

            except (RuntimeError, asyncio.CancelledError, aiohttp.ClientError) as e:
                logger.warning(f"Polling loop closure/network notice for chat {chat_id}: {e}. Retrying in 3s...")
                await asyncio.sleep(3.0)
                continue
            except Exception as e:
                logger.warning(f"Polling exception in run_monitor for chat {chat_id}: {e}. Retrying in 5s...")
                await asyncio.sleep(5.0)
                continue

            await asyncio.sleep(5.0)

    async def _cmd_status_async(self, chat_id: str | int):
        try:
            if chat_id not in ACTIVE_TRACKS and str(chat_id) not in ACTIVE_TRACKS:
                await asyncio.to_thread(
                    self.client.send_message,
                    chat_id,
                    "ℹ️ No active match being tracked. Use /matches to select a match and set an alert.",
                    "HTML", False
                )
                return

            key = chat_id if chat_id in ACTIVE_TRACKS else str(chat_id)
            data = ACTIVE_TRACKS[key]

            team_name = data.get("team", data.get("team_name", "Match"))
            match_slug = data.get("match_slug")

            odds_data = None
            try:
                if match_slug:
                    odds_data = await global_exchange_scraper.get_live_odds_data_for_match_slug_async(match_slug, team_name)
                else:
                    odds_data = await global_exchange_scraper.get_live_odds_data_for_team_async(team_name)
            except Exception as ex:
                logger.warning(f"Live odds fetch notice in /status for chat {chat_id}: {ex}")

            if odds_data:
                if isinstance(odds_data.get("target_odd"), (int, float)):
                    data["current_odd"] = odds_data["target_odd"]
                if odds_data.get("opponent_team"):
                    data["opponent_team"] = odds_data["opponent_team"]
                    data["opponent_odd"] = odds_data["opponent_odd"]
                if odds_data.get("target_team"):
                    data["target_team_clean"] = odds_data["target_team"]

            target_display = (data.get("target_team_clean") or team_name).upper()
            opp_display = (data.get("opponent_team") or "").upper()
            match_title = f"{target_display} vs {opp_display}" if opp_display else target_display

            target = data.get("target", data.get("target_odd", 0.0))
            curr = data.get("current_odd")
            if curr is None or not isinstance(curr, (int, float)) or curr <= 1.01:
                curr = data.get("last_seen_odd") or data.get("entry_odd") or data.get("entry")

            # If still None, await actual current odd directly
            if curr is None or not isinstance(curr, (int, float)) or curr <= 1.01:
                try:
                    fresh_odd = await global_exchange_scraper.get_live_odd_for_team_async(team_name)
                    if fresh_odd and isinstance(fresh_odd, (int, float)) and fresh_odd > 1.01:
                        curr = fresh_odd
                        data["current_odd"] = fresh_odd
                        data["last_seen_odd"] = fresh_odd
                except Exception as ex:
                    logger.warning(f"Could not await live odd for status fallback: {ex}")

            stake = data.get("stake", 1000.0)

            if isinstance(curr, (int, float)) and curr > 1.01:
                ind_curr = format_indian_odds(curr)
                curr_str = f"{curr:.2f} (<code>{ind_curr}</code>)"
            else:
                target_ind_val = format_indian_odds(target) if isinstance(target, (int, float)) and target > 1.01 else "1-2"
                curr_str = f"{target:.2f} (<code>{target_ind_val}</code>)"

            target_ind = format_indian_odds(target) if isinstance(target, (int, float)) and target > 1.01 else f"{target:.2f}"

            msg = (
                f"📊 <b>Active Tracking Status</b>\n\n"
                f"🏏 <b>Match:</b> {html.escape(match_title)}\n"
                f"🟢 <b>Tracked Team:</b> {html.escape(target_display)}\n"
                f"🎯 <b>Target Odd:</b> {target:.2f} (<code>{target_ind}</code>)\n"
                f"⚡ <b>Current Live Bhav:</b> {curr_str}\n"
                f"💰 <b>Stake:</b> ₹{stake:,.0f}\n\n"
                f"<i>Alert will trigger automatically when target is reached.</i>"
            )

            await asyncio.to_thread(self.client.send_message, chat_id, msg, "HTML", False)
        except Exception as e:
            logger.error(f"Error in /status handler for chat {chat_id}: {e}")
            try:
                fallback_msg = (
                    "📊 <b>Active Tracking Status (Cached)</b>\n\n"
                    "⚡ <i>Tracking session active. Target monitoring in progress...</i>"
                )
                await asyncio.to_thread(self.client.send_message, chat_id, fallback_msg, "HTML", False)
            except Exception:
                pass

    async def _cmd_stop_async(self, chat_id: str | int, args: list):
        team_filter = args[0].lower().strip() if args else None
        
        removed = []
        keys_to_clear = []
        for key, data in list(ACTIVE_TRACKS.items()):
            if str(key) == str(chat_id) or key == chat_id:
                team_name = data.get("team", data.get("team_name", ""))
                if team_filter is None or team_filter in str(team_name).lower():
                    removed.append(str(team_name))
                    keys_to_clear.append(key)

        for key in keys_to_clear:
            if key in ACTIVE_TRACKS:
                del ACTIVE_TRACKS[key]
            task_key = str(key)
            if task_key in self.tracking_tasks:
                self.tracking_tasks[task_key].cancel()
                del self.tracking_tasks[task_key]

        save_active_jobs(ACTIVE_TRACKS)
        self.engine.remove_track(chat_id, team_filter)

        if removed:
            await asyncio.to_thread(
                self.client.send_message,
                chat_id,
                f"🛑 <b>TRACKING CANCELLED!</b>\nCleared active tracking session for: <b>{', '.join(removed)}</b>",
                "HTML", False
            )
        else:
            await asyncio.to_thread(self.client.send_message, chat_id, "ℹ️ No active match being tracked.", "HTML", False)

    async def _cmd_mute_async(self, chat_id: str | int, args: list, muted: bool):
        team_filter = args[0].lower().strip() if args else None
        
        updated = []
        if chat_id in ACTIVE_TRACKS or str(chat_id) in ACTIVE_TRACKS:
            key = chat_id if chat_id in ACTIVE_TRACKS else str(chat_id)
            data = ACTIVE_TRACKS[key]
            team_name = data.get("team", data.get("team_name", ""))
            if team_filter is None or team_filter in str(team_name).lower():
                data["muted"] = muted
                updated.append(str(team_name))
                save_active_jobs(ACTIVE_TRACKS)

        self.engine.set_mute(chat_id, team_filter, muted)

        action = "Muted" if muted else "Unmuted"
        if updated:
            await asyncio.to_thread(self.client.send_message, chat_id, f"🔕 <b>{action} alert notifications for:</b> {', '.join(updated)}", "HTML", False)
        else:
            await asyncio.to_thread(self.client.send_message, chat_id, "ℹ️ No active tracks found to update.", "HTML", False)

    async def _cmd_setodd_async(self, chat_id: str | int, args: list):
        if len(args) < 2:
            await asyncio.to_thread(self.client.send_message, chat_id, "Format: <code>/setodd &lt;team&gt; &lt;new_odd&gt;</code>\nExample: <code>/setodd India 0.25</code>", "HTML", False)
            return
        team, val_str = args[0], args[1]
        try:
            val = float(val_str)
            global_exchange_scraper.set_team_odd_override(team, val)

            if chat_id in ACTIVE_TRACKS or str(chat_id) in ACTIVE_TRACKS:
                key = chat_id if chat_id in ACTIVE_TRACKS else str(chat_id)
                data = ACTIVE_TRACKS[key]
                if team.lower().strip() in str(data.get("team", "")).lower():
                    data["current_odd"] = val
                    data["last_seen_odd"] = val

            await asyncio.to_thread(self.client.send_message, chat_id, f"⚡ <b>Scraper Odd Updated:</b> {team} odd set to <b>{val:.2f}</b>", "HTML", False)
        except ValueError:
            await asyncio.to_thread(self.client.send_message, chat_id, "❌ Invalid odd number.", "HTML", False)

    def _on_odds_update(self, job: TrackJob, live_odd: float):
        pass

    def _on_alert_trigger(self, job: TrackJob, live_odd: float):
        pass

    def _on_error(self, job: TrackJob, err_msg: str):
        pass


if __name__ == "__main__":
    token = BOT_TOKEN
    if token:
        bot = TelegramOddsBot(token)
        bot.start()
    else:
        print("Error: No bot token provided. Exiting.")
