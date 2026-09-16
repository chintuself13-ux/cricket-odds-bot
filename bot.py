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

from odds_engine import MultiTrackOddsEngine, TrackJob, global_odds_data_engine
from exchange_scraper import global_exchange_scraper, format_indian_odds


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


def load_allowed_users() -> Dict[int, Optional[datetime]]:
    allowed: Dict[int, Optional[datetime]] = {DEFAULT_ADMIN_ID: None}
    env_admin = os.environ.get("ADMIN_ID")
    if env_admin:
        try:
            allowed[int(env_admin)] = None
        except ValueError:
            pass

    if os.path.exists(ALLOWED_USERS_FILE):
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


ALLOWED_USERS: Dict[int, Optional[datetime]] = load_allowed_users()

ACTIVE_JOBS_FILE = "active_jobs.json"


def save_active_jobs(active_tracks: Dict[Any, Dict[str, Any]]):
    """Saves ACTIVE_TRACKS dictionary to active_jobs.json for restart persistence."""
    try:
        data = {}
        for key, track in active_tracks.items():
            clean_item = {}
            for k, v in track.items():
                if isinstance(v, (str, int, float, bool, type(None))):
                    clean_item[k] = v
            data[str(key)] = clean_item

        with open(ACTIVE_JOBS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not save active_jobs.json: {e}")


def load_active_jobs() -> Dict[Any, Dict[str, Any]]:
    """Loads saved tracking jobs from active_jobs.json on startup."""
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
            del ALLOWED_USERS[user_id]
            save_allowed_users(ALLOWED_USERS)
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

            if cb_id:
                asyncio.create_task(asyncio.to_thread(
                    self.client.answer_callback_query,
                    cb_id,
                    text="🛑 Siren Alarm Stopped!",
                    show_alert=True
                ))

            if cb_data in ["stop_alarm", "mute_alarm", "stop_tracking"] and chat_id:
                asyncio.create_task(self._cmd_stop_async(chat_id, []))
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
            if cmd in ["/allow", "/revoke", "/users"]:
                if not is_admin:
                    asyncio.create_task(asyncio.to_thread(self.client.send_message, chat_id, "❌ Only the Admin can use this command.", "HTML", False))
                    return
                if cmd == "/allow":
                    asyncio.create_task(self._cmd_allow_async(chat_id, parts[1:]))
                elif cmd == "/revoke":
                    asyncio.create_task(self._cmd_revoke_async(chat_id, parts[1:]))
                elif cmd == "/users":
                    asyncio.create_task(self._cmd_users_async(chat_id))
                return

            # 2. Public /buy command
            if cmd == "/buy":
                asyncio.create_task(self._cmd_buy_async(chat_id))
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
                asyncio.create_task(self._cmd_matches_async(chat_id))
            elif cmd == "/track":
                asyncio.create_task(self._cmd_track_async(chat_id, parts[1:]))
            elif cmd == "/status":
                asyncio.create_task(self._cmd_status_async(chat_id))
            elif cmd == "/stop":
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
            # Handle non-command messages (Text or Screenshot/Photo proof submissions)
            if not is_admin:
                asyncio.create_task(self._handle_user_proof_submission(chat_id, user_id, from_user, text, photo, caption))

    def _handle_unauthorized_start(self, chat_id: str | int, user_id: int, from_user: Dict[str, Any]):
        first_name = html.escape(str(from_user.get("first_name", "")))
        last_name = html.escape(str(from_user.get("last_name", "")))
        full_name = f"{first_name} {last_name}".strip() or "User"
        uname = from_user.get("username")
        username = f"@{html.escape(str(uname))}" if uname else "No username"

        # 1. Send welcome & free trial info to user
        user_msg = (
            f"👋 <b>Welcome to Live Cricket Odds & Alert Bot!</b>\n\n"
            f"🎁 Get a <b>3-Day Free Trial</b> to track live cricket exchange odds, "
            f"automated cashout calculations, and high priority alerts!\n\n"
            f"📩 Admin has been notified to activate your 3-day trial.\n"
            f"💳 Or send <code>/buy</code> to purchase a 30-day subscription for ₹50."
        )
        asyncio.create_task(asyncio.to_thread(self.client.send_message, chat_id, user_msg, "HTML", False))

        # 2. Notify Admin with 1-tap /allow <user_id> 3d command
        admin_alert = (
            f"🔔 <b>NEW USER TRIAL REQUEST!</b>\n\n"
            f"👤 <b>Name:</b> {full_name}\n"
            f"🏷️ <b>Username:</b> {username}\n"
            f"🆔 <b>User ID:</b> <code>{user_id}</code>\n\n"
            f"👉 Tap to activate 3-Day Trial:\n"
            f"<code>/allow {user_id} 3d</code>"
        )
        asyncio.create_task(asyncio.to_thread(self.client.send_message, DEFAULT_ADMIN_ID, admin_alert, "HTML", False))

    async def _cmd_buy_async(self, chat_id: str | int):
        qr_url = "https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=upi://pay?pa=rajdiljeet@fam%26pn=OddsTracker%26am=50%26cu=INR"
        caption = (
            "💳 Subscription Plan: ₹50 / 30 Days\n"
            "UPI ID: <code>rajdiljeet@fam</code> (tap to copy)\n\n"
            "Scan the QR or copy the UPI ID to pay ₹50.\n"
            "After payment, send the screenshot or 12-digit UTR number right here in this chat."
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
            ALLOWED_USERS[target_user_id] = expiry_dt
            save_allowed_users(ALLOWED_USERS)

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
                del ALLOWED_USERS[target_user_id]
                save_allowed_users(ALLOWED_USERS)
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

    def _cmd_start(self, chat_id: str | int, user_id: Optional[str | int] = None):
        is_admin = (user_id and (user_id == DEFAULT_ADMIN_ID or str(user_id) == str(DEFAULT_ADMIN_ID)))
        admin_extra = (
            "• <code>/allow &lt;user_id&gt; &lt;duration&gt;</code> — Grant access (e.g. <code>/allow 12345678 30d</code>)\n"
            "• <code>/revoke &lt;user_id&gt;</code> — Revoke user authorization\n"
            "• <code>/users</code> — View all active users & remaining days\n"
        ) if is_admin else ""

        help_text = (
            "💰 <b>LIVE CRICKET ODDS ALERT & CASHOUT CALCULATOR BOT</b> ⚡\n\n"
            "Monitor live cricket exchange rates with an <b>automated Green Book Cashout Calculator</b>! "
            "Get <b>exact Lay stakes & guaranteed profit numbers</b> sent directly in alert messages!\n\n"
            "📌 <b>COMMAND SYNTAX:</b>\n\n"
            "• <code>/track &lt;team&gt; &lt;target_odd&gt; [stake]</code>\n"
            "  <i>Auto Live Entry:</i> <code>/track Zimbabwe 2.40 1000</code>\n"
            "  <i>Quick Track:</i> <code>/track Australia 1.06 1000</code>\n\n"
            "• <code>/matches</code> — View live matches with Win Chance %, Decimal & Indian (Paresh/Lagan) Odds\n"
            "• <code>/status</code> — View active tracked matches, live odds, elapsed time & instant cashout\n"
            "• <code>/buy</code> — View ₹50 subscription plan & payment QR\n"
            "• <code>/mute</code> — Silence repeating alert notifications without ending tracking\n"
            "• <code>/unmute</code> — Resume alert notifications\n"
            "• <code>/stop [team]</code> — Stop tracking a match and clear background task\n"
            f"{admin_extra}"
            "• <code>/setodd &lt;team&gt; &lt;odd&gt;</code> — Modify live odd for instant testing (e.g., <code>/setodd India 0.25</code>)\n"
        )
        self.client.send_message(chat_id, help_text)

    async def _cmd_matches_async(self, chat_id: str | int):
        raw_matches = await global_exchange_scraper.fetch_live_matches_async()

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
            msg = "🏏 Currently no live matches are in-play. Please check back when a live game starts."
            await asyncio.to_thread(self.client.send_message, chat_id, msg, "HTML", False)
            return

        lines = []
        for m in matches:
            home_team = m.get("home_team", "Team 1")
            away_team = m.get("away_team", "Team 2")
            odds_list = m.get("odds", [])

            # Extract home and away odds strictly preserving official fixture order
            home_odd = next((o for o in odds_list if o.get("name") == home_team), odds_list[0] if len(odds_list) > 0 else None)
            away_odd = next((o for o in odds_list if o.get("name") == away_team), odds_list[1] if len(odds_list) > 1 else None)

            if not home_odd or not away_odd:
                continue

            # Determine favorite based on lower decimal back odd
            is_home_fav = home_odd.get("back", 99.0) <= away_odd.get("back", 99.0)

            home_fav_tag = " (Fav)" if is_home_fav else ""
            away_fav_tag = " (Fav)" if not is_home_fav else ""

            home_ind = format_indian_odds(home_odd['back'], home_odd['lay'])
            away_ind = format_indian_odds(away_odd['back'], away_odd['lay'])

            home_win = int(round((1.0 / max(1.01, home_odd['back'])) * 100))
            away_win = int(round((1.0 / max(1.01, away_odd['back'])) * 100))

            home_line = f"• {home_team}{home_fav_tag}: {home_ind} ({home_odd['back']:.2f} / {home_odd['lay']:.2f}) | {home_win}% Win"
            away_line = f"• {away_team}{away_fav_tag}: {away_ind} ({away_odd['back']:.2f} / {away_odd['lay']:.2f}) | {away_win}% Win"

            home_target = round(max(1.02, home_odd['back'] - 0.07), 2) if is_home_fav else round(max(1.10, home_odd['back'] * 0.53), 2)
            away_target = round(max(1.02, away_odd['back'] - 0.07), 2) if not is_home_fav else round(max(1.10, away_odd['back'] * 0.53), 2)

            lines.append(
                f"🏏 <b>{home_team} vs {away_team}</b> (In-Play)\n"
                f"{home_line}\n"
                f"{away_line}\n\n"
                f"👉 <code>/track {home_team} {home_target:.2f} 1000</code>\n"
                f"👉 <code>/track {away_team} {away_target:.2f} 1000</code>"
            )

        await asyncio.to_thread(self.client.send_message, chat_id, "\n\n".join(lines), "HTML", False)

    async def _cmd_track_async(self, chat_id: str | int, args: list):
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
            "last_alert_time": 0.0
        }
        ACTIVE_TRACKS[chat_id] = track_entry
        save_active_jobs(ACTIVE_TRACKS)

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

                # 1. Fetch live odds
                odds_data = await global_exchange_scraper.get_live_odds_data_for_team_async(team_name)
                latest_odd = odds_data.get("target_odd") if odds_data else None

                if latest_odd is not None and isinstance(latest_odd, (int, float)) and latest_odd > 1.01:
                    track["current_odd"] = latest_odd
                    track["last_seen_odd"] = latest_odd
                    if odds_data.get("opponent_team"):
                        track["opponent_team"] = odds_data["opponent_team"]
                        track["opponent_odd"] = odds_data["opponent_odd"]
                    if odds_data.get("target_team"):
                        track["target_team_clean"] = odds_data["target_team"]
                    if track.get("entry") is None or not isinstance(track.get("entry"), (int, float)) or track.get("entry") <= 1.01:
                        track["entry"] = latest_odd
                        track["entry_odd"] = latest_odd
                else:
                    logger.info(f"Waiting for valid live odds for chat {chat_id} ({team_name}). Maintaining active tracking state.")

                # 2. Check threshold trigger ONLY if real valid numeric odd (> 1.01) is scraped
                curr = track.get("current_odd")
                target_val = track.get("target", track.get("target_odd", 0.0))
                target_display = (track.get("target_team_clean") or team_name).upper()

                if curr is not None and isinstance(curr, (int, float)) and curr > 1.01 and curr <= target_val:
                    track["status"] = "TRIGGERED"
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
        if chat_id not in ACTIVE_TRACKS and str(chat_id) not in ACTIVE_TRACKS:
            await asyncio.to_thread(
                self.client.send_message,
                chat_id,
                "ℹ️ No active tracking job."
            )
            return

        key = chat_id if chat_id in ACTIVE_TRACKS else str(chat_id)
        data = ACTIVE_TRACKS[key]

        team_name = data.get("team", data.get("team_name", "Match"))
        odds_data = await global_exchange_scraper.get_live_odds_data_for_team_async(team_name)

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
        opp_odd = data.get("opponent_odd")

        opp_line = ""
        if opp_display and isinstance(opp_odd, (int, float)):
            opp_ind = format_indian_odds(opp_odd)
            opp_line = f"⚔️ <b>Opponent Odd ({html.escape(opp_display)}):</b> {opp_odd:.2f} (<code>{opp_ind}</code>)\n"

        target = data.get("target", data.get("target_odd", 0.0))
        curr = data.get("current_odd")
        entry = data.get("entry", data.get("entry_odd"))
        stake = data.get("stake", 1000.0)

        if isinstance(curr, (int, float)) and curr > 1.01:
            ind_str = format_indian_odds(curr)
            curr_str = f"<b>{curr:.2f}</b> (<code>{ind_str}</code>)"
            curr_val = curr
        else:
            curr_str = "<i>Fetching live feed...</i>"
            curr_val = None

        if isinstance(entry, (int, float)) and entry > 1.01:
            ind_entry = format_indian_odds(entry)
            entry_str = f"<b>{entry:.2f}</b> (<code>{ind_entry}</code>)"
            entry_val = entry
        else:
            entry_str = "<i>Pending...</i>"
            entry_val = None

        target_ind = format_indian_odds(target) if isinstance(target, (int, float)) and target > 1.01 else ""

        if curr_val and entry_val:
            lay_stake = round((entry_val * stake) / max(0.01, curr_val), 2)
            profit = round(lay_stake - stake, 2)
            cashout_block = (
                f"💰 <b>Live Cashout Calculation:</b>\n"
                f"👉 Lay <b>₹{lay_stake:,.2f}</b> on <b>{html.escape(target_display)}</b> @ <b>{curr_val:.2f}</b>\n"
                f"💚 Guaranteed Profit: <b>+₹{profit:,.2f}</b>\n"
            )
        else:
            cashout_block = (
                f"💰 <b>Live Cashout Calculation:</b>\n"
                f"<i>Waiting for live exchange odds feed...</i>\n"
            )

        status_icon = "🚨 ALERT TRIGGERED" if data.get("status") == "TRIGGERED" else "🟢 ACTIVE"
        mute_str = " (🔕 Muted)" if data.get("muted") else ""

        elapsed = int(time.time() - data.get("start_time", time.time()))
        mins, secs = divmod(elapsed, 60)
        hrs, mins = divmod(mins, 60)
        elapsed_str = f"{hrs}h {mins}m {secs}s" if hrs > 0 else (f"{mins}m {secs}s" if mins > 0 else f"{secs}s")

        msg = (
            f"📊 <b>LIVE TRACKING STATUS</b>\n\n"
            f"🎯 <b>Target:</b> {html.escape(target_display)} &lt;= {target:.2f} (<code>{target_ind}</code>){mute_str}\n"
            f"📈 <b>Current Live Odd ({html.escape(target_display)}):</b> {curr_str}\n"
            f"{opp_line}"
            f"📊 <b>Entry Odd (Auto):</b> {entry_str} | <b>Stake:</b> ₹{stake:,.0f}\n"
            f"📡 <b>Data Source:</b> ⚡ Live Exchange Feed\n"
            f"⏱ <b>Elapsed Tracking Time:</b> {elapsed_str}\n\n"
            f"{cashout_block}"
            f"Status: {status_icon}"
        )

        await asyncio.to_thread(self.client.send_message, chat_id, msg, "HTML", False)

    async def _cmd_stop_async(self, chat_id: str | int, args: list):
        team_filter = args[0].lower().strip() if args else None
        
        removed = []
        if chat_id in ACTIVE_TRACKS or str(chat_id) in ACTIVE_TRACKS:
            key = chat_id if chat_id in ACTIVE_TRACKS else str(chat_id)
            data = ACTIVE_TRACKS[key]
            team_name = data.get("team", data.get("team_name", ""))
            if team_filter is None or team_filter in str(team_name).lower():
                removed.append(str(team_name))
                del ACTIVE_TRACKS[key]
                save_active_jobs(ACTIVE_TRACKS)
                if str(key) in self.tracking_tasks:
                    self.tracking_tasks[str(key)].cancel()
                    del self.tracking_tasks[str(key)]

        self.engine.remove_track(chat_id, team_filter)

        if removed:
            await asyncio.to_thread(self.client.send_message, chat_id, f"⏹ <b>Stopped tracking for:</b> {', '.join(removed)}", "HTML", False)
        else:
            await asyncio.to_thread(self.client.send_message, chat_id, "ℹ️ No matching active tracks found to stop.", "HTML", False)

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
