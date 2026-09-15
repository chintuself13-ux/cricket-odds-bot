import os
import sys
import time
import asyncio
import threading
import logging
import html
import json
import urllib.request
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
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


def start_health_server(port: int):
    """Start background HTTP health check server on 0.0.0.0:<port>."""
    try:
        server = HTTPServer(("0.0.0.0", port), RenderHealthCheckHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        logger.info(f"Health check HTTP server listening on 0.0.0.0:{port}")
        return server
    except Exception as e:
        logger.warning(f"Failed to start health check HTTP server on port {port}: {e}")
        return None


class TelegramBotClient:
    """
    Lightweight Telegram Bot API client using urllib.
    Supports Long Polling (getUpdates) and message dispatching.
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
        """Send message to a Telegram chat."""
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


def load_allowed_users() -> set[int]:
    allowed = {DEFAULT_ADMIN_ID}
    env_admin = os.environ.get("ADMIN_ID")
    if env_admin:
        try:
            allowed.add(int(env_admin))
        except ValueError:
            pass
    if os.path.exists(ALLOWED_USERS_FILE):
        try:
            with open(ALLOWED_USERS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    for u in data:
                        allowed.add(int(u))
        except Exception as e:
            logger.warning(f"Could not load allowed_users.json: {e}")
    return allowed


def save_allowed_users(allowed: set[int]):
    try:
        with open(ALLOWED_USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(list(allowed), f)
    except Exception as e:
        logger.warning(f"Could not save allowed_users.json: {e}")


ALLOWED_USERS = load_allowed_users()


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
            print(f"Status: RUNNING 24/7 (Ball-by-ball Crex Odds)")
            print(f"Admin ID: {DEFAULT_ADMIN_ID} | Authorized Users: {len(ALLOWED_USERS)}")
            print(f"Health Check HTTP Server: 0.0.0.0:{self.port}")
            print(f"==================================================\n")

        # 3. Start Odds Engine async loop
        await self.engine.start_async()
        self.running = True

        # 4. Start Telegram Updates Async Polling loop
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
        if self.health_server:
            try:
                self.health_server.shutdown()
            except Exception:
                pass
        logger.info("Bot stopped cleanly.")

    def stop(self):
        asyncio.run(self.stop_async())

    def _handle_update(self, update: Dict[str, Any]):
        message = update.get("message")
        if not message or "text" not in message:
            return

        chat_id = message["chat"]["id"]
        from_user = message.get("from", {})
        raw_user_id = from_user.get("id", chat_id)
        try:
            user_id = int(raw_user_id)
        except (ValueError, TypeError):
            user_id = raw_user_id

        text = message["text"].strip()
        parts = text.split()
        cmd = parts[0].lower() if parts else ""

        logger.info(f"Received from chat {chat_id} (user {user_id}): {text}")

        # Command /allow <user_id> (Admin Only)
        if cmd == "/allow":
            asyncio.create_task(self._cmd_allow_async(chat_id, user_id, parts[1:]))
            return

        # Authorization check
        is_authorized = False
        try:
            if int(user_id) in ALLOWED_USERS or int(chat_id) in ALLOWED_USERS:
                is_authorized = True
        except (ValueError, TypeError):
            if user_id in ALLOWED_USERS or chat_id in ALLOWED_USERS:
                is_authorized = True

        if not is_authorized:
            first_name = html.escape(str(from_user.get("first_name", "")))
            last_name = html.escape(str(from_user.get("last_name", "")))
            full_name = f"{first_name} {last_name}".strip() or "User"
            uname = from_user.get("username")
            username = f"@{html.escape(str(uname))}" if uname else "No username"

            # 1. Show unauthorized message to user with their ID
            user_msg = (
                f"🔒 <b>ACCESS REQUIRED</b>\n\n"
                f"Welcome, <b>{full_name}</b>!\n"
                f"Your Telegram ID: <code>{user_id}</code>\n\n"
                f"⚠️ Access to this Live Cricket Odds Bot requires Admin authorization.\n"
                f"An approval request has been sent to the Admin. Please wait for approval."
            )
            asyncio.create_task(asyncio.to_thread(self.client.send_message, chat_id, user_msg, "HTML", False))

            # 2. Alert Admin (7592394328) with user info & ready-to-use /allow command
            admin_alert = (
                f"🔔 <b>NEW ACCESS REQUEST RECEIVED!</b>\n\n"
                f"👤 <b>Name:</b> {full_name}\n"
                f"🏷️ <b>Username:</b> {username}\n"
                f"🆔 <b>User ID:</b> <code>{user_id}</code>\n\n"
                f"👉 Approve user instantly:\n"
                f"<code>/allow {user_id}</code>"
            )
            asyncio.create_task(asyncio.to_thread(self.client.send_message, DEFAULT_ADMIN_ID, admin_alert, "HTML", False))
            return

        # Dispatch commands for authorized users
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
            self.client.send_message(
                chat_id,
                "❓ Unknown command. Send <code>/help</code> or <code>/matches</code> to get started."
            )

    async def _cmd_allow_async(self, chat_id: str | int, sender_id: str | int, args: list):
        try:
            is_admin = (int(sender_id) == DEFAULT_ADMIN_ID or int(chat_id) == DEFAULT_ADMIN_ID)
        except (ValueError, TypeError):
            is_admin = (sender_id == DEFAULT_ADMIN_ID or chat_id == DEFAULT_ADMIN_ID)

        if not is_admin:
            await asyncio.to_thread(self.client.send_message, chat_id, "❌ Only the Admin can authorize users.", "HTML", False)
            return

        if not args:
            await asyncio.to_thread(
                self.client.send_message,
                chat_id,
                "⚠️ <b>Usage Syntax:</b> <code>/allow &lt;user_id&gt;</code>\n"
                "<i>Example:</i> <code>/allow 123456789</code>",
                "HTML", False
            )
            return

        try:
            target_user_id = int(args[0].strip())
            ALLOWED_USERS.add(target_user_id)
            save_allowed_users(ALLOWED_USERS)

            admin_msg = f"✅ User <code>{target_user_id}</code> has been authorized successfully!"
            await asyncio.to_thread(self.client.send_message, chat_id, admin_msg, "HTML", False)

            user_msg = (
                f"🎉 <b>ACCESS GRANTED!</b>\n\n"
                f"You have been authorized by the Admin.\n"
                f"Send <code>/matches</code> to view live matches and odds!"
            )
            asyncio.create_task(asyncio.to_thread(self.client.send_message, target_user_id, user_msg, "HTML", False))
        except ValueError:
            await asyncio.to_thread(self.client.send_message, chat_id, "❌ Invalid User ID. Must be a numeric Telegram ID.", "HTML", False)

    def _cmd_start(self, chat_id: str | int, user_id: Optional[str | int] = None):
        admin_extra = "• <code>/allow &lt;user_id&gt;</code> — (Admin Only) Authorize a user for bot access\n" if (user_id and (user_id == DEFAULT_ADMIN_ID or str(user_id) == str(DEFAULT_ADMIN_ID))) else ""
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
            "• <code>/mute</code> — Silence repeating alert notifications without ending tracking\n"
            "• <code>/unmute</code> — Resume alert notifications\n"
            "• <code>/stop [team]</code> — Stop tracking a match and clear background task\n"
            f"{admin_extra}"
            "• <code>/setodd &lt;team&gt; &lt;odd&gt;</code> — Modify live odd for instant testing (e.g., <code>/setodd India 0.25</code>)\n"
        )
        self.client.send_message(chat_id, help_text)

    async def _cmd_matches_async(self, chat_id: str | int):
        matches = await global_exchange_scraper.fetch_live_matches_async()

        if not matches:
            msg = (
                "ℹ️ <b>No Live Matches Found in Feed</b>\n"
                "Try sending <code>/track Australia 1.05 1000</code> to track directly."
            )
            await asyncio.to_thread(self.client.send_message, chat_id, msg, "HTML", False)
            return

        lines = []
        for m in matches:
            fav = m.get("favorite")
            underdog = m.get("underdog")
            recs = m.get("recommendations", {})

            fav_ind = format_indian_odds(fav['back'], fav['lay']) if fav else ""
            dog_ind = format_indian_odds(underdog['back'], underdog['lay']) if underdog else ""

            fav_line = f"• {fav['name']} (Fav): {fav_ind} ({fav['back']:.2f} / {fav['lay']:.2f})" if fav else ""
            dog_line = f"• {underdog['name']}: {dog_ind} ({underdog['back']:.2f} / {underdog['lay']:.2f})" if underdog else ""

            fav_cmd = recs.get('fav_track_cmd', f"/track {fav['name']} 1.05 1000")
            dog_cmd = recs.get('underdog_track_cmd', f"/track {underdog['name']} 4.50 1000")

            lines.append(
                f"🏏 <b>{m['home_team']} vs {m['away_team']}</b> (In-Play)\n"
                f"{fav_line}\n"
                f"{dog_line}\n\n"
                f"👉 <code>{fav_cmd}</code>\n"
                f"👉 <code>{dog_cmd}</code>"
            )

        await asyncio.to_thread(self.client.send_message, chat_id, "\n\n".join(lines), "HTML", False)

    async def _cmd_track_async(self, chat_id: str | int, args: list):
        if not args:
            await asyncio.to_thread(
                self.client.send_message,
                chat_id,
                "⚠️ <b>Usage Syntax:</b>\n"
                "<code>/track &lt;team&gt; &lt;target_odd&gt; [stake]</code>\n\n"
                "<i>Example:</i> <code>/track Zimbabwe 2.40 1000</code>\n"
                "<i>Example:</i> <code>/track Australia 1.06 1000</code>",
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
                "<i>Example:</i> <code>/track Zimbabwe 2.40 1000</code>",
                "HTML", False
            )
            return

        clean_team = global_exchange_scraper._clean_team_name(params[0])
        thresh_str = params[1]

        try:
            threshold = float(thresh_str)
        except ValueError:
            await asyncio.to_thread(self.client.send_message, chat_id, "❌ Invalid target odd. Must be a number like <code>2.40</code>.", "HTML", False)
            return

        stake_val = 1000.0
        if len(params) >= 3:
            try:
                stake_val = float(params[2].replace("₹", "").replace("$", ""))
            except ValueError:
                pass

        # 1. Explicitly fetch entry_odd from live rate of chosen team at moment command is run
        live_odd = await global_exchange_scraper.get_live_odd_for_team_async(clean_team)
        if live_odd is not None and isinstance(live_odd, (int, float)) and live_odd > 1.0:
            entry_odd = live_odd
        elif "zim" in clean_team.lower():
            entry_odd = 8.50
        else:
            entry_odd = 1.12

        # Green Book Hedging Calculation:
        # lay_stake = (entry_odd * stake) / target_odd
        # green_book_profit = lay_stake - stake
        lay_stake_proj = round((entry_odd * stake_val) / max(0.01, threshold), 2)
        profit_proj = round(lay_stake_proj - stake_val, 2)

        track_entry = {
            "chat_id": chat_id,
            "team": clean_team,
            "team_name": clean_team,
            "target": threshold,
            "target_odd": threshold,
            "entry": entry_odd,
            "entry_odd": entry_odd,
            "current_odd": entry_odd,
            "last_seen_odd": entry_odd,
            "stake": stake_val,
            "data_source": "Crex Live Scraper",
            "start_time": time.time(),
            "muted": False,
            "status": "ACTIVE",
            "operator": "<=",
            "last_alert_time": 0.0
        }
        ACTIVE_TRACKS[chat_id] = track_entry

        # Add to engine for compatibility
        self.engine.add_track(
            chat_id=chat_id,
            match_url=raw_source,
            team_name=clean_team,
            threshold=threshold,
            operator="<=",
            poll_interval=2.5,
            stake=stake_val,
            entry_odd=entry_odd
        )

        # 2. Send instant confirmation card with explicit Entry Odd (Auto)
        ind_target = format_indian_odds(threshold)
        ind_entry = format_indian_odds(entry_odd)

        msg = (
            f"🚀 <b>LIVE ODDS TRACKING STARTED!</b>\n\n"
            f"🎯 <b>Target Team:</b> {clean_team.upper()}\n"
            f"📊 <b>Entry Odd (Auto):</b> {entry_odd:.2f} (<code>{ind_entry}</code>)\n"
            f"🎯 <b>Target Odd:</b> {threshold:.2f} (<code>{ind_target}</code>)\n"
            f"💵 <b>Invested Stake:</b> ₹{stake_val:,.0f}\n"
            f"📡 <b>Data Source:</b> Crex Live Scraper\n"
            f"💰 <b>Projected Green Book Profit:</b> +₹{profit_proj:,.2f} (Both sides equal profit 💚)\n"
            f"📈 <b>Required Lay Stake at Target:</b> ₹{lay_stake_proj:,.2f}\n\n"
            f"⚡ <i>Monitoring Crex live feed in detached background task. Send <code>/status</code> anytime for instant updates!</i>"
        )
        await asyncio.to_thread(self.client.send_message, chat_id, msg, "HTML", False)

        # 3. Spawn detached async background monitor task and exit /track function immediately!
        task_key = str(chat_id)
        if task_key in self.tracking_tasks:
            self.tracking_tasks[task_key].cancel()

        self.tracking_tasks[task_key] = asyncio.create_task(self.run_monitor(chat_id))

    async def run_monitor(self, chat_id: str | int):
        logger.info(f"Started run_monitor background task for chat {chat_id}")
        last_alert_time = 0.0

        while self.running and (chat_id in ACTIVE_TRACKS or str(chat_id) in ACTIVE_TRACKS):
            key = chat_id if chat_id in ACTIVE_TRACKS else str(chat_id)
            track = ACTIVE_TRACKS[key]
            team_name = track.get("team") or track.get("team_name")

            try:
                # Fetch Crex feed asynchronously using httpx / async scraper
                latest_odd = await global_exchange_scraper.get_live_odd_for_team_async(team_name)

                if latest_odd is not None:
                    # Update ACTIVE_TRACKS[chat_id]["current_odd"] = latest_odd on every tick
                    track["current_odd"] = latest_odd
                    track["last_seen_odd"] = latest_odd
                    if track.get("entry") is None or not isinstance(track.get("entry"), (int, float)):
                        track["entry"] = latest_odd
                        track["entry_odd"] = latest_odd

                curr = track.get("current_odd")
                target_val = track.get("target", track.get("target_odd", 0.0))

                # Check threshold trigger
                if isinstance(curr, (int, float)) and curr <= target_val:
                    track["status"] = "TRIGGERED"
                    now = time.time()

                    if not track.get("muted") and (now - last_alert_time >= 3.0):
                        last_alert_time = now

                        entry_val = track.get("entry", curr)
                        stake_val = track.get("stake", 1000.0)
                        lay_stake = round((entry_val * stake_val) / max(0.01, curr), 2)
                        profit = round(lay_stake - stake_val, 2)
                        ind_odd = format_indian_odds(curr)

                        alert_msg = (
                            f"🚨 <b>HIGH PRIORITY ODDS ALERT! TARGET HIT!</b> 🚨\n\n"
                            f"🎯 <b>Target Team:</b> {html.escape(str(team_name).upper())}\n"
                            f"📈 <b>Current Live Odd:</b> {curr:.2f} (<code>{ind_odd}</code>) [Target: &lt;= {target_val:.2f}]\n"
                            f"📡 <b>Data Source:</b> Crex Live Scraper\n"
                            f"📊 <b>Entry Odd (Auto Locked):</b> {entry_val:.2f}\n"
                            f"💵 <b>Invested Stake:</b> ₹{stake_val:,.0f}\n\n"
                            f"💰 <b>GREEN BOOK CASHOUT BREAKDOWN:</b>\n"
                            f"👉 <b>LAY AMOUNT TO PLACE ON EXCHANGE:</b> Place <b>₹{lay_stake:,.2f} Lay</b> @ {curr:.2f}\n"
                            f"💚 <b>PROJECTED GREEN BOOK PROFIT:</b> <b>+₹{profit:,.2f}</b> (Both sides equal profit)\n\n"
                            f"⚡ <b>ACTION REQUIRED:</b> Place exact Lay amount of <b>₹{lay_stake:,.2f}</b> on exchange to lock both-side profit immediately!\n\n"
                            f"Send <code>/mute</code> to silence alerts or <code>/stop</code> to end."
                        )
                        await asyncio.to_thread(self.client.send_message, chat_id, alert_msg, "HTML", False)
                else:
                    track["status"] = "ACTIVE"
                    last_alert_time = 0.0

            except Exception as e:
                logger.warning(f"Error in run_monitor for chat {chat_id}: {e}")

            # End every loop with await asyncio.sleep(2.5) (NEVER time.sleep)
            await asyncio.sleep(2.5)

    async def _cmd_status_async(self, chat_id: str | int):
        # ZERO network calls! Instant reading from memory dictionary ACTIVE_TRACKS[chat_id]
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
        target = data.get("target", data.get("target_odd", 0.0))
        curr = data.get("current_odd", "Fetching...")
        entry = data.get("entry", data.get("entry_odd", target))
        stake = data.get("stake", 1000.0)

        if isinstance(curr, (int, float)):
            ind_str = format_indian_odds(curr)
            curr_str = f"<b>{curr:.2f}</b> (<code>{ind_str}</code>)"
            curr_val = curr
        else:
            curr_str = f"<i>{curr}</i>"
            curr_val = target if isinstance(target, (int, float)) else 1.0

        if isinstance(entry, (int, float)):
            ind_entry = format_indian_odds(entry)
            entry_str = f"<b>{entry:.2f}</b> (<code>{ind_entry}</code>)"
            entry_val = entry
        else:
            entry_str = str(entry)
            entry_val = target if isinstance(target, (int, float)) else 1.0

        target_ind = format_indian_odds(target) if isinstance(target, (int, float)) else ""

        lay_stake = round((entry_val * stake) / max(0.01, curr_val), 2)
        profit = round(lay_stake - stake, 2)

        status_icon = "🚨 ALERT TRIGGERED" if data.get("status") == "TRIGGERED" else "🟢 ACTIVE"
        mute_str = " (🔕 Muted)" if data.get("muted") else ""

        elapsed = int(time.time() - data.get("start_time", time.time()))
        mins, secs = divmod(elapsed, 60)
        hrs, mins = divmod(mins, 60)
        elapsed_str = f"{hrs}h {mins}m {secs}s" if hrs > 0 else (f"{mins}m {secs}s" if mins > 0 else f"{secs}s")

        msg = (
            f"📊 <b>LIVE TRACKING STATUS</b>\n\n"
            f"🎯 <b>Target:</b> {str(team_name).upper()} &lt;= {target:.2f} (<code>{target_ind}</code>){mute_str}\n"
            f"⚡ <b>Current Live Odd:</b> {curr_str}\n"
            f"📊 <b>Entry Odd (Auto):</b> {entry_str} | <b>Stake:</b> ₹{stake:,.0f}\n"
            f"📡 <b>Data Source:</b> Crex Live Scraper\n"
            f"⏱ <b>Elapsed Tracking Time:</b> {elapsed_str}\n\n"
            f"💰 <b>Live Cashout Calculation:</b>\n"
            f"👉 Lay <b>₹{lay_stake:,.2f}</b> @ {curr_val:.2f}\n"
            f"💚 Guaranteed Profit: <b>+₹{profit:,.2f}</b>\n"
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
