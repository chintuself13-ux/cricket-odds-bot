import urllib.request
import urllib.error
import json
import threading
import time
import html
import os
import sys
import asyncio
import logging
from typing import Dict, List, Optional, Callable, Any, Tuple

# Force UTF-8 encoding for Windows console output
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

import urllib.parse
from exchange_scraper import global_exchange_scraper, format_indian_odds

logger = logging.getLogger("OddsEngine")

def send_telegram_alert(
    bot_token: str,
    chat_id: str | int,
    message: str,
    reply_markup: Optional[Dict[str, Any]] = None
) -> Tuple[bool, str]:
    """
    Dispatches HTML alert message to Telegram Bot API with notification sound enabled and inline keyboard.
    """
    if not bot_token or not chat_id:
        err = "Bot token or Chat ID missing"
        print(f"[SEND ERROR] {err}", flush=True)
        return False, err

    try:
        clean_chat_id = int(str(chat_id).strip())
    except ValueError:
        clean_chat_id = str(chat_id).strip()

    url = f"https://api.telegram.org/bot{str(bot_token).strip()}/sendMessage"
    payload = {
        "chat_id": clean_chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "disable_notification": False
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            res_body = json.loads(resp.read().decode("utf-8"))
            if res_body.get("ok"):
                print(f"[SEND SUCCESS] Alert delivered to chat_id: {clean_chat_id}", flush=True)
                return True, "Alert sent successfully"
            else:
                desc = res_body.get("description", "Unknown API error")
                print(f"[SEND ERROR] Telegram API rejected message: {desc}", flush=True)
                return False, f"Telegram API error: {desc}"
    except Exception as e:
        print(f"[SEND ERROR] Exception while sending alert to Telegram: {e}", flush=True)
        return False, str(e)

class OddsDataEngine:
    """
    Permanent default real-time engine using Crex Live Scraper (exchange_scraper.py).
    Fetches real-time ball-by-ball rates directly with zero request quotas or API key limits.
    """
    def __init__(self, api_keys_str: str = ""):
        self.api_keys = []
        self.current_key_index = 0
        self.use_scraper_fallback = True
        self._lock = threading.Lock()

    def get_source_label(self) -> str:
        return "Live Scraper"

    def fetch_live_odd(self, team_name: str) -> Tuple[Optional[float], str]:
        odd = global_exchange_scraper.get_live_odd_for_team(team_name)
        return odd, "Live Scraper"

global_odds_data_engine = OddsDataEngine()

class TrackJob:
    def __init__(
        self,
        chat_id: str | int,
        match_url: str,
        team_name: str,
        threshold: float,
        operator: str = "<=",
        poll_interval: float = 2.5,
        stake: float = 1000.0,
        entry_odd: Optional[float] = None
    ):
        try:
            self.chat_id = int(str(chat_id).strip())
        except ValueError:
            self.chat_id = str(chat_id).strip()

        self.match_url = match_url.strip()
        self.team_name = team_name.strip()
        self.threshold = float(threshold)
        self.operator = operator if operator in ["<=", ">="] else "<="
        self.poll_interval = max(1.0, float(poll_interval))

        self.stake = float(stake) if stake else 1000.0
        self.entry_odd = float(entry_odd) if entry_odd else None

        self.muted = False
        self.last_odd: Optional[float] = None
        self.last_checked_time: float = 0.0
        self.last_alert_time: float = 0.0
        self.start_time: float = time.time()
        self.consecutive_errors = 0
        self.status = "active"
        self.siren_burst_done = False
        self.last_siren_time = 0.0

    def get_elapsed_time_str(self) -> str:
        elapsed = int(time.time() - self.start_time)
        mins, secs = divmod(elapsed, 60)
        hrs, mins = divmod(mins, 60)
        if hrs > 0:
            return f"{hrs}h {mins}m {secs}s"
        elif mins > 0:
            return f"{mins}m {secs}s"
        else:
            return f"{secs}s"

    def calculate_cashout(self, current_odd: float) -> Dict[str, Any]:
        entry = self.entry_odd if self.entry_odd else (self.last_odd or current_odd)
        if not entry or not current_odd or current_odd <= 0:
            return {"lay_stake": 0.0, "profit": 0.0, "entry_odd": entry or current_odd, "stake": self.stake}

        lay_stake = round((entry * self.stake) / current_odd, 2)
        guaranteed_profit = round(lay_stake - self.stake, 2)

        return {
            "entry_odd": round(entry, 2),
            "target_odd": round(current_odd, 2),
            "stake": round(self.stake, 2),
            "lay_stake": lay_stake,
            "profit": guaranteed_profit
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chat_id": self.chat_id,
            "match_url": self.match_url,
            "team_name": self.team_name,
            "threshold": self.threshold,
            "operator": self.operator,
            "poll_interval": self.poll_interval,
            "stake": self.stake,
            "entry_odd": self.entry_odd,
            "muted": self.muted,
            "last_odd": self.last_odd,
            "status": self.status
        }


class MultiTrackOddsEngine:
    def __init__(
        self,
        bot_token: str = "",
        on_odds_update: Optional[Callable[[TrackJob, float], None]] = None,
        on_alert_trigger: Optional[Callable[[TrackJob, float], None]] = None,
        on_error: Optional[Callable[[TrackJob, str], None]] = None
    ):
        self.bot_token = bot_token
        self.on_odds_update = on_odds_update
        self.on_alert_trigger = on_alert_trigger
        self.on_error = on_error

        self.jobs: Dict[str, TrackJob] = {}
        self._lock = threading.Lock()
        self._running = False
        self._thread = None
        self._loop_task: Optional[asyncio.Task] = None

    async def start_async(self):
        if self._running:
            return
        self._running = True
        self._loop_task = asyncio.create_task(self._loop_async())

    async def stop_async(self):
        self._running = False
        if self._loop_task:
            self._loop_task.cancel()

    async def _loop_async(self):
        while self._running:
            with self._lock:
                current_jobs = list(self.jobs.values())

            now = time.time()
            for job in current_jobs:
                if not self._running:
                    break
                if now - job.last_checked_time >= job.poll_interval:
                    job.last_checked_time = now
                    asyncio.create_task(self._process_job_async(job))

            await asyncio.sleep(0.5)

    async def _process_job_async(self, job: TrackJob):
        await asyncio.to_thread(self._process_job, job)

    def add_track(
        self,
        chat_id: str | int,
        match_url: str,
        team_name: str,
        threshold: float,
        operator: str = "<=",
        poll_interval: float = 2.5,
        stake: float = 1000.0,
        entry_odd: Optional[float] = None
    ) -> TrackJob:
        key = f"{str(chat_id).strip()}:{team_name.lower().strip()}"

        if entry_odd is None:
            live_now, _ = global_odds_data_engine.fetch_live_odd(team_name)
            if live_now:
                entry_odd = live_now

        job = TrackJob(chat_id, match_url, team_name, threshold, operator, poll_interval, stake, entry_odd)

        with self._lock:
            self.jobs[key] = job

        print(f"[UNLIMITED TRACK ADDED] Chat ID: {job.chat_id} | Team: {job.team_name} | Threshold: {job.operator} {job.threshold:.2f} | Stake: ₹{job.stake} | Auto Entry Odd: {job.entry_odd}", flush=True)
        return job

    def remove_track(self, chat_id: str | int, team_name: Optional[str] = None) -> List[str]:
        chat_id_str = str(chat_id).strip()
        removed = []
        with self._lock:
            keys_to_del = []
            for key, job in self.jobs.items():
                if str(job.chat_id) == chat_id_str:
                    if team_name is None or team_name.lower().strip() in job.team_name.lower():
                        keys_to_del.append(key)
                        removed.append(job.team_name)
            for k in keys_to_del:
                del self.jobs[k]
        return removed

    def set_mute(self, chat_id: str | int, team_name: Optional[str], muted: bool) -> List[str]:
        chat_id_str = str(chat_id).strip()
        updated = []
        with self._lock:
            for job in self.jobs.values():
                if str(job.chat_id) == chat_id_str:
                    if team_name is None or team_name.lower().strip() in job.team_name.lower():
                        job.muted = muted
                        updated.append(job.team_name)
        return updated

    def get_user_jobs(self, chat_id: str | int) -> List[TrackJob]:
        chat_id_str = str(chat_id).strip()
        with self._lock:
            return [job for job in self.jobs.values() if str(job.chat_id) == chat_id_str]

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            with self._lock:
                current_jobs = list(self.jobs.values())

            now = time.time()
            for job in current_jobs:
                if not self._running:
                    break
                if now - job.last_checked_time >= job.poll_interval:
                    job.last_checked_time = now
                    threading.Thread(target=self._process_job, args=(job,), daemon=True).start()

            time.sleep(0.5)

    def _process_job(self, job: TrackJob):
        try:
            live_odd, source_name = global_odds_data_engine.fetch_live_odd(job.team_name)

            if live_odd is None and job.match_url.startswith("http"):
                req = urllib.request.Request(
                    job.match_url,
                    headers={
                        "User-Agent": "ExchangeScraper/3.0",
                        "Cache-Control": "no-cache, no-store, must-revalidate",
                        "Pragma": "no-cache",
                        "Expires": "0"
                    }
                )
                with urllib.request.urlopen(req, timeout=4.0) as resp:
                    raw_bytes = resp.read()
                    data = json.loads(raw_bytes.decode("utf-8"))
                    live_odd = self.extract_odd(data, job.team_name)

            job.consecutive_errors = 0

            if job.entry_odd is None and live_odd is not None:
                job.entry_odd = live_odd

            print(f"[DEBUG POLL] Team: {job.team_name} | Live Odd: {live_odd} | Target: {job.operator} {job.threshold:.2f} | Source: {source_name}", flush=True)

            if live_odd is not None:
                job.last_odd = live_odd

                if self.on_odds_update:
                    self.on_odds_update(job, live_odd)

                triggered = False
                if job.operator == "<=" and live_odd <= job.threshold:
                    triggered = True
                elif job.operator == ">=" and live_odd >= job.threshold:
                    triggered = True

                if triggered:
                    job.status = "triggered"
                    now = time.time()

                    cashout = job.calculate_cashout(live_odd)
                    op_html = "&lt;=" if job.operator == "<=" else "&gt;="
                    ind_odd = format_indian_odds(live_odd)

                    reply_markup = {
                        "inline_keyboard": [
                            [{"text": "⏹️ Stop Siren / Cashout Done", "callback_data": f"stop_siren:{job.chat_id}:{job.team_name}"}]
                        ]
                    }

                    alert_msg = (
                        f"🚨🚨🚨 <b>ODDS ALERT HIT!</b> 🚨🚨🚨\n\n"
                        f"🎯 <b>Target Team:</b> {html.escape(job.team_name.upper())}\n"
                        f"📈 <b>Current Live Odd:</b> {live_odd:.2f} (<code>{ind_odd}</code>) [Target: {op_html} {job.threshold:.2f}]\n"
                        f"💰 <b>Stake:</b> ₹{cashout['stake']:,.0f}\n"
                        f"💵 <b>LAY AMOUNT TO PLACE:</b> Lay <b>₹{cashout['lay_stake']:,.2f}</b> @ {live_odd:.2f}\n"
                        f"💚 <b>PROJECTED NET PROFIT:</b> +₹{cashout['profit']:,.2f} (Green Book Profit)\n\n"
                        f"⚡ Tap button below to stop siren & complete cashout!"
                    )

                    if not job.muted:
                        # 1. Fire rapid burst of 5 siren messages on initial target hit
                        if not job.siren_burst_done:
                            job.siren_burst_done = True
                            job.last_siren_time = now
                            for _ in range(5):
                                try:
                                    if self.bot_token and job.chat_id:
                                        send_telegram_alert(self.bot_token, job.chat_id, alert_msg, reply_markup=reply_markup)
                                        time.sleep(0.2)
                                except Exception as e:
                                    print(f"[SIREN BURST ERROR] {e}", flush=True)
                        # 2. Continuous reminder siren every 10 seconds until user taps stop button
                        elif now - job.last_siren_time >= 10.0:
                            job.last_siren_time = now
                            try:
                                if self.bot_token and job.chat_id:
                                    send_telegram_alert(self.bot_token, job.chat_id, alert_msg, reply_markup=reply_markup)
                            except Exception as e:
                                print(f"[REMINDER SIREN ERROR] {e}", flush=True)

                    if self.on_alert_trigger:
                        self.on_alert_trigger(job, live_odd)
                else:
                    if job.status == "triggered":
                        print(f"[ALERT RESET] {job.team_name} odd recovered ({live_odd:.2f} > {job.threshold:.2f}). State reset for fresh alerts.", flush=True)
                    job.status = "active"
                    job.last_alert_time = 0.0
                    job.siren_burst_done = False

            else:
                job.status = "warning"
                print(f"[WARNING] Team '{job.team_name}' not found in exchange feed.", flush=True)
                if self.on_error:
                    self.on_error(job, f"Team '{job.team_name}' not found in exchange feed.")

        except Exception as e:
            job.consecutive_errors += 1
            job.status = "error"
            print(f"[SCRAPER ERROR] {job.team_name}: {e}", flush=True)
            if self.on_error:
                self.on_error(job, f"Scraper Error ({job.consecutive_errors}): {str(e)}")

    @staticmethod
    def extract_odd(data: Any, team_name: str) -> Optional[float]:
        target_lower = team_name.lower().strip()

        teams_list = []
        if isinstance(data, dict):
            for key in ["teams", "runners", "participants", "odds", "data"]:
                if key in data and isinstance(data[key], list):
                    teams_list = data[key]
                    break
        elif isinstance(data, list):
            teams_list = data

        for item in teams_list:
            if isinstance(item, dict):
                name = str(item.get("name", item.get("team", item.get("runnerName", "")))).lower().strip()
                if target_lower in name or name in target_lower:
                    for odd_key in ["odd", "odds", "live_odd", "back", "price"]:
                        if odd_key in item and item[odd_key] is not None:
                            try:
                                return float(item[odd_key])
                            except (ValueError, TypeError):
                                pass

        return None
