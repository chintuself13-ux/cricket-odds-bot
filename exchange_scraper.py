import urllib.request
import urllib.parse
import re
import json
import os
import time
import threading
import logging
import asyncio
import aiohttp
from typing import Dict, List, Any, Optional, Tuple

try:
    from curl_cffi.requests import AsyncSession
    HAS_CURL_CFFI = True
except ImportError:
    AsyncSession = None
    HAS_CURL_CFFI = False

logger = logging.getLogger("ExchangeScraper")

DEFAULT_DOMAIN = "https://reddybook.info"
CURRENT_EXCHANGE_URL = os.getenv("EXCHANGE_URL", DEFAULT_DOMAIN).rstrip("/")
BASE_URL = CURRENT_EXCHANGE_URL
BASE_EXCHANGE_URL = CURRENT_EXCHANGE_URL

DEFAULT_CATALOG_URL = "https://yellow-voice-8690.chintuself13.workers.dev"
CURRENT_CATALOG_URL = os.getenv("CATALOG_URL", DEFAULT_CATALOG_URL)

DEFAULT_ODDS_URL = "https://odd.ocric99.com"
CURRENT_ODDS_URL = os.getenv("ODDS_URL", DEFAULT_ODDS_URL)

DEFAULT_CATALOG_ENDPOINTS = [
    "https://yellow-voice-8690.chintuself13.workers.dev",
    "https://api.reddybook.club/api/guest/event_list",
    "https://api.reddybook.art/api/guest/event_list",
    "https://api.cricbet99.win/api/guest/event_list",
    "https://odds.cricketbet9.com/api/guest/event_list"
]

BLOCKED_KEYWORDS = [
    "srl", "simulated", "virtual", "cyber", "e-cricket", 
    "electronic", "table cricket", "t10", "simulated reality"
]

MARKET_CACHE: Dict[str, Dict[str, Any]] = {}
MARKET_CACHE_LOCK = asyncio.Lock()
WS_TASK: Optional[asyncio.Task] = None
WS_URL = "wss://odd.ocric99.com/ws/getMarketDataNew"


def _fetch_direct_exchange():
    url = CURRENT_CATALOG_URL if CURRENT_CATALOG_URL and "cricbet" in CURRENT_CATALOG_URL else "https://api.cricbet99.click/api/guest/event_list"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://reddybook.club",
        "Referer": "https://reddybook.club/"
    }
    if HAS_CURL_CFFI:
        from curl_cffi import requests as cffi_requests
        r = cffi_requests.get(url, headers=headers, impersonate="chrome120", timeout=15)
        return r.json()
    else:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode('utf-8'))


async def get_live_matches() -> List[Dict[str, Any]]:
    """
    Fetches live cricket matches directly using curl_cffi Chrome 120 TLS impersonation.
    """
    try:
        payload = await asyncio.to_thread(_fetch_direct_exchange)
    except Exception as e:
        logger.error(f"[SCRAPER] Direct exchange fetch failed: {e}")
        return []

    if not payload or not isinstance(payload, dict):
        logger.error("[SCRAPER] Empty or invalid payload from direct exchange.")
        return []

    data_block = payload.get("data") if isinstance(payload, dict) else {}
    events = data_block.get("events", []) if isinstance(data_block, dict) else payload.get("events", [])
    if not isinstance(events, list):
        events = []

    logger.info(f"[SCRAPER] Total events parsed: {len(events)}")

    real_matches = []
    seen = set()

    for item in events:
        if not isinstance(item, dict):
            continue

        if str(item.get("event_type_id", "")).strip() != "4":
            continue

        name = item.get("name", "").strip()
        comp = item.get("competition_name", "").strip().lower()
        lower_name = name.lower()

        if not (" v " in lower_name or " vs " in lower_name):
            continue

        if any(b in lower_name for b in BLOCKED_KEYWORDS) or any(b in comp for b in BLOCKED_KEYWORDS):
            continue

        m_id = str(item.get("market_id") or item.get("event_id") or "").strip()
        if not m_id or m_id in seen:
            continue

        seen.add(m_id)

        parts = re.split(r'\s+(?:vs|v|-)\s+', name, flags=re.IGNORECASE)
        home_team = parts[0].strip() if len(parts) >= 2 else "Team 1"
        away_team = parts[1].strip() if len(parts) >= 2 else "Team 2"

        real_matches.append({
            "id": m_id,
            "match_id": m_id,
            "match_slug": m_id,
            "name": name,
            "title": name,
            "home_team": home_team,
            "away_team": away_team,
            "competition": item.get("competition_name", ""),
            "odds": []
        })

    logger.info(f"[SCRAPER] Returning {len(real_matches)} valid cricket matches")
    return real_matches


def set_list_url(new_url: str) -> str:
    global CURRENT_CATALOG_URL
    if new_url:
        url_str = new_url.strip()
        if not (url_str.startswith("http://") or url_str.startswith("https://")):
            url_str = "https://" + url_str
        CURRENT_CATALOG_URL = url_str
        logger.info(f"CURRENT_CATALOG_URL updated dynamically to: {CURRENT_CATALOG_URL}")
    return CURRENT_CATALOG_URL


def set_odds_url(new_url: str) -> str:
    global CURRENT_ODDS_URL, WS_URL
    if new_url:
        url_str = new_url.strip()
        if not (url_str.startswith("http://") or url_str.startswith("https://") or url_str.startswith("ws://") or url_str.startswith("wss://")):
            url_str = "https://" + url_str
        CURRENT_ODDS_URL = url_str
        WS_URL = CURRENT_ODDS_URL.replace("https://", "wss://").replace("http://", "ws://")
        logger.info(f"CURRENT_ODDS_URL updated dynamically to: {CURRENT_ODDS_URL} (WS: {WS_URL})")
    return CURRENT_ODDS_URL


async def update_market_cache_from_payload(data: Any):
    """
    Parses JSON payload frames from WebSocket stream and updates global MARKET_CACHE.
    """
    global MARKET_CACHE
    if not data:
        return

    items_to_cache = []
    if isinstance(data, list):
        items_to_cache = data
    elif isinstance(data, dict):
        m_list = (
            data.get("matches") or data.get("data") or
            data.get("result") or data.get("items") or
            data.get("gmarket") or []
        )
        if isinstance(m_list, list) and m_list:
            items_to_cache = m_list
        elif "runners" in data:
            items_to_cache = [data]

    if not items_to_cache:
        return

    async with MARKET_CACHE_LOCK:
        for item in items_to_cache:
            if isinstance(item, dict):
                m_id = str(
                    item.get("match_id") or item.get("matchId") or item.get("eventId") or
                    item.get("marketId") or item.get("id") or item.get("gmarket") or ""
                )
                if not m_id:
                    event_name = item.get("event_name") or item.get("eventName") or item.get("matchName") or ""
                    m_id = event_name.lower().replace(" ", "-") if event_name else str(len(MARKET_CACHE) + 1)

                MARKET_CACHE[m_id] = item


async def run_websocket_listener():
    """
    WebSocket listener permanently disabled. Matches & market data fetched via standard HTTP GET.
    """
    logger.info("WebSocket listener is disabled.")
    return


def set_exchange_url(new_url: str) -> str:
    global CURRENT_EXCHANGE_URL, BASE_URL, BASE_EXCHANGE_URL
    if new_url:
        url_str = new_url.strip()
        if not (url_str.startswith("http://") or url_str.startswith("https://")):
            url_str = "https://" + url_str
        CURRENT_EXCHANGE_URL = url_str.rstrip("/")
        BASE_URL = CURRENT_EXCHANGE_URL
        BASE_EXCHANGE_URL = CURRENT_EXCHANGE_URL
        logger.info(f"CURRENT_EXCHANGE_URL updated dynamically to: {CURRENT_EXCHANGE_URL}")
    return CURRENT_EXCHANGE_URL

def set_base_url(new_url: str) -> str:
    return set_exchange_url(new_url)

TEAM_ABBREVIATIONS = {
    "AUS": "Australia",
    "ZIM": "Zimbabwe",
    "IND": "India",
    "AFG": "Afghanistan",
    "ENG": "England",
    "SL": "Sri Lanka",
    "PAK": "Pakistan",
    "SA": "South Africa",
    "NZ": "New Zealand",
    "WI": "West Indies",
    "BAN": "Bangladesh",
    "BOT": "Botswana",
    "KEN": "Kenya"
}

def convert_paresh_to_decimal(val: float) -> float:
    if val is None or val <= 0:
        return 1.01
    if val < 1.0:
        return round(1.0 + val, 2)
    elif val < 100.0:
        return round(1.0 + (val / 100.0), 2)
    else:
        return round(1.0 + (val / 100.0), 2)

def is_team_match(feed_name: Optional[str], target_name: Optional[str]) -> bool:
    if not feed_name or not target_name:
        return False
    feed = str(feed_name).lower().replace("-", " ").replace("_", " ").strip()
    target = str(target_name).lower().replace("-", " ").replace("_", " ").strip()
    if feed == target or feed in target or target in feed:
        return True
    feed_words = [w for w in feed.split() if len(w) > 2]
    target_words = [w for w in target.split() if len(w) > 2]
    return any(word in target for word in feed_words) or any(word in feed for word in target_words)

def format_indian_odds(back_odd: Optional[float], lay_odd: Optional[float] = None) -> str:
    """
    Format odds cleanly:
    - If decimal < 2.0 (Favourite): int(round((decimal - 1.0) * 100)) paise.
    - If decimal >= 2.0 (Underdog): f"{decimal:.2f} rate".
    """
    if back_odd is None or not isinstance(back_odd, (int, float)) or back_odd <= 1.0:
        return ""

    if back_odd < 2.00:
        back_paise = int(round((back_odd - 1.0) * 100))
        return f"{back_paise} paise"
    else:
        return f"{back_odd:.2f} rate"

class ExchangeScraperEngine:
    """
    Direct Reddybook / Diamond Exchange API Engine.
    Exclusively fetches live cricket matches from Reddybook / Diamond Exchange endpoints.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self.last_fetch_time = 0
        self.last_fetch_status = 200
        self.http_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": CURRENT_EXCHANGE_URL,
            "Accept": "application/json, text/plain, */*"
        }
        self.manual_overrides: Dict[str, float] = {}
        self._aiohttp_session: Optional[aiohttp.ClientSession] = None

    def _get_override(self, team_name: str) -> Optional[float]:
        if not team_name:
            return None
        t_clean = team_name.lower().strip()
        with self._lock:
            for k, v in self.manual_overrides.items():
                if k.lower().strip() in t_clean or t_clean in k.lower().strip():
                    return v
        return None

    def set_override(self, team_name: str, odd: float):
        with self._lock:
            self.manual_overrides[team_name.strip()] = float(odd)

    def set_team_odd_override(self, team_name: str, odd: float):
        self.set_override(team_name, odd)

    def clear_overrides(self):
        with self._lock:
            self.manual_overrides.clear()

    def _clean_team_name(self, name: str) -> str:
        if not name:
            return ""
        c = name.strip()
        for abbr, full in TEAM_ABBREVIATIONS.items():
            if c.upper() == abbr:
                return full
        return c

    def _is_strict_team_match(self, name1: str, name2: str) -> bool:
        return is_team_match(name1, name2)

    async def get_aiohttp_session(self) -> aiohttp.ClientSession:
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        if (
            self._aiohttp_session is not None
            and not self._aiohttp_session.closed
            and current_loop
            and getattr(self._aiohttp_session, "_loop", None) != current_loop
        ):
            try:
                await self._aiohttp_session.close()
            except Exception:
                pass
            self._aiohttp_session = None

        if self._aiohttp_session is None or self._aiohttp_session.closed:
            connector = aiohttp.TCPConnector(ssl=False, limit=10, keepalive_timeout=30)
            timeout = aiohttp.ClientTimeout(total=8.0, connect=3.0)
            self._aiohttp_session = aiohttp.ClientSession(
                headers=self.http_headers,
                connector=connector,
                timeout=timeout
            )
        return self._aiohttp_session

    async def close_async(self):
        if self._aiohttp_session and not self._aiohttp_session.closed:
            try:
                await self._aiohttp_session.close()
            except Exception:
                pass
            self._aiohttp_session = None

    def _parse_exchange_match(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(item, dict):
            return None

        match_id = str(item.get("match_id") or item.get("matchId") or item.get("eventId") or item.get("marketId") or item.get("id") or item.get("gmarket") or "")
        
        event_name = item.get("event_name") or item.get("eventName") or item.get("matchName") or item.get("title") or item.get("name") or ""
        home_team = self._clean_team_name(item.get("home_team") or item.get("homeTeam") or item.get("team1") or item.get("runner1") or "")
        away_team = self._clean_team_name(item.get("away_team") or item.get("awayTeam") or item.get("team2") or item.get("runner2") or "")

        if (not home_team or not away_team) and event_name:
            parts = re.split(r'\s+(?:vs|v|-)\s+', event_name, flags=re.IGNORECASE)
            if len(parts) >= 2:
                home_team = self._clean_team_name(parts[0])
                away_team = self._clean_team_name(parts[1])

        runners = item.get("runners") or item.get("runnersBook") or item.get("outcomes") or item.get("runner") or item.get("section") or []

        if (not home_team or not away_team) and isinstance(runners, list) and len(runners) >= 2:
            r0 = runners[0] if isinstance(runners[0], dict) else {}
            r1 = runners[1] if isinstance(runners[1], dict) else {}
            t0 = r0.get("runnerName") or r0.get("name") or r0.get("nation") or r0.get("team")
            t1 = r1.get("runnerName") or r1.get("name") or r1.get("nation") or r1.get("team")
            if t0 and t1:
                home_team = self._clean_team_name(t0)
                away_team = self._clean_team_name(t1)

        if not home_team or not away_team or home_team.lower() == away_team.lower():
            return None

        odds_arr = []

        for idx, runner in enumerate(runners):
            if isinstance(runner, dict):
                r_name = self._clean_team_name(runner.get("runnerName") or runner.get("name") or runner.get("nation") or runner.get("team") or (home_team if idx == 0 else away_team))
                
                back_price = None
                back_data = (
                    runner.get("b1") if runner.get("b1") is not None else
                    runner.get("back") if runner.get("back") is not None else
                    runner.get("price") if runner.get("price") is not None else
                    runner.get("rate") if runner.get("rate") is not None else
                    runner.get("backPrice")
                )
                if isinstance(back_data, list) and back_data:
                    first_b = back_data[0]
                    if isinstance(first_b, dict):
                        back_price = first_b.get("price") or first_b.get("rate") or first_b.get("b1")
                    elif isinstance(first_b, (int, float)):
                        back_price = float(first_b)
                elif isinstance(back_data, (int, float)):
                    back_price = float(back_data)

                lay_price = None
                lay_data = (
                    runner.get("l1") if runner.get("l1") is not None else
                    runner.get("lay") if runner.get("lay") is not None else
                    runner.get("layPrice")
                )
                if isinstance(lay_data, list) and lay_data:
                    first_l = lay_data[0]
                    if isinstance(first_l, dict):
                        lay_price = first_l.get("price") or first_l.get("rate") or first_l.get("l1")
                    elif isinstance(first_l, (int, float)):
                        lay_price = float(first_l)
                elif isinstance(lay_data, (int, float)):
                    lay_price = float(lay_data)

                override = self._get_override(r_name)
                if override:
                    back_price = override

                if back_price is not None and isinstance(back_price, (int, float)) and back_price > 0:
                    if back_price < 1.0:
                        dec_price = round(1.0 + back_price, 2)
                    elif back_price >= 1.0 and back_price < 100.0 and "." not in str(back_price) and back_price < 1.5:
                        dec_price = round(1.0 + (back_price / 100.0), 2)
                    else:
                        dec_price = round(back_price, 2)

                    odds_arr.append({
                        "name": r_name,
                        "back": dec_price,
                        "lay": lay_price,
                        "price": dec_price,
                        "indian_odds": format_indian_odds(dec_price)
                    })

        print(f"[EXCHANGE DEBUG] Match: {home_team} vs {away_team} | Raw Odds: {odds_arr}", flush=True)

        return {
            "id": match_id or f"{home_team}-vs-{away_team}",
            "match_id": match_id or f"{home_team}-vs-{away_team}",
            "match_slug": match_id or f"{home_team}-vs-{away_team}",
            "title": f"{home_team} vs {away_team}",
            "sport": "Reddybook / Diamond Exchange",
            "status": "In-Play",
            "is_finished": False,
            "winner": None,
            "home_team": home_team,
            "away_team": away_team,
            "odds": odds_arr
        }

    def start_websocket_listener_task(self) -> Optional[asyncio.Task]:
        """
        WebSocket listener disabled.
        """
        return None

    async def fetch_live_exchange_matches(self) -> List[Dict[str, Any]]:
        """
        Fetches live in-play cricket matches directly from Cricbet99 event listing feed with SRL exclusion.
        """
        return await get_live_matches()

    async def fetch_live_matches_async(self) -> List[Dict[str, Any]]:
        return await self.fetch_live_exchange_matches()

    def fetch_live_matches(self) -> List[Dict[str, Any]]:
        import asyncio
        try:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop and loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    return pool.submit(lambda: asyncio.run(self.fetch_live_exchange_matches())).result(timeout=10.0)
            else:
                return asyncio.run(self.fetch_live_exchange_matches())
        except Exception as e:
            logger.warning(f"Error in sync fetch_live_matches: {e}")
            return []

    async def scrape_single_match_by_slug_async(self, slug: str) -> Optional[Dict[str, Any]]:
        matches = await self.fetch_live_exchange_matches()
        for m in matches:
            if m.get("id") == slug or m.get("match_id") == slug or slug in str(m.get("id")):
                return m
        return matches[0] if matches else None

    async def get_live_odds_data_for_match_slug_async(self, match_slug: str, team_name: str) -> Dict[str, Any]:
        override = self._get_override(team_name)
        target_clean = self._clean_team_name(team_name).upper()

        m = await self.scrape_single_match_by_slug_async(match_slug)
        if not m:
            return await self.get_live_odds_data_for_team_async(team_name)

        odds = m.get("odds", [])
        for idx, outcome in enumerate(odds):
            name = outcome["name"]
            if self._is_strict_team_match(team_name, name):
                target_team = outcome["name"]
                target_odd = override if override else outcome.get("back")
                target_lay = outcome.get("lay")

                opponent_outcome = odds[1 - idx] if len(odds) > 1 else None
                opponent_team = opponent_outcome["name"] if opponent_outcome else None
                opponent_odd = opponent_outcome["back"] if opponent_outcome else None
                opponent_lay = opponent_outcome.get("lay") if opponent_outcome else None

                return {
                    "target_team": target_team,
                    "target_odd": target_odd,
                    "target_lay": target_lay,
                    "opponent_team": opponent_team,
                    "opponent_odd": opponent_odd,
                    "opponent_lay": opponent_lay,
                    "match_title": m.get("title"),
                    "match_slug": m.get("match_slug") or match_slug,
                    "match_id": m.get("match_id") or match_slug,
                    "is_finished": False,
                    "status": "In-Play",
                    "winner": None
                }

        return {
            "target_team": target_clean,
            "target_odd": override,
            "target_lay": None,
            "opponent_team": None,
            "opponent_odd": None,
            "opponent_lay": None,
            "match_title": m.get("title"),
            "match_slug": m.get("match_slug") or match_slug,
            "match_id": m.get("match_id") or match_slug,
            "is_finished": False,
            "status": "In-Play",
            "winner": None
        }

    async def get_live_odds_data_for_team_async(self, team_name: str) -> Dict[str, Any]:
        override = self._get_override(team_name)
        target_clean = self._clean_team_name(team_name).upper()
        matches = await self.fetch_live_exchange_matches()

        for m in matches:
            odds = m.get("odds", [])
            for idx, outcome in enumerate(odds):
                name = outcome["name"]
                if self._is_strict_team_match(team_name, name):
                    target_team = outcome["name"]
                    target_odd = override if override else outcome.get("back")
                    target_lay = outcome.get("lay")

                    opponent_outcome = odds[1 - idx] if len(odds) > 1 else None
                    opponent_team = opponent_outcome["name"] if opponent_outcome else None
                    opponent_odd = opponent_outcome["back"] if opponent_outcome else None
                    opponent_lay = opponent_outcome.get("lay") if opponent_outcome else None

                    return {
                        "target_team": target_team,
                        "target_odd": target_odd,
                        "target_lay": target_lay,
                        "opponent_team": opponent_team,
                        "opponent_odd": opponent_odd,
                        "opponent_lay": opponent_lay,
                        "match_title": m.get("title"),
                        "match_slug": m.get("match_slug") or m.get("id"),
                        "match_id": m.get("match_id") or m.get("id"),
                        "is_finished": False,
                        "status": "In-Play",
                        "winner": None
                    }

        if override:
            return {
                "target_team": self._clean_team_name(team_name),
                "target_odd": override,
                "target_lay": round(override + 0.05, 2),
                "opponent_team": None,
                "opponent_odd": None,
                "opponent_lay": None,
                "match_title": None
            }

        return {
            "target_team": self._clean_team_name(team_name),
            "target_odd": None,
            "target_lay": None,
            "opponent_team": None,
            "opponent_odd": None,
            "opponent_lay": None,
            "match_title": None
        }

    async def get_live_odd_for_team_async(self, team_name: str) -> Optional[float]:
        override = self._get_override(team_name)
        if override:
            return override

        matches = await self.fetch_live_exchange_matches()
        for m in matches:
            for outcome in m.get("odds", []):
                name = outcome["name"]
                if self._is_strict_team_match(team_name, name):
                    return outcome["back"]
        return None

    def get_live_odd_for_team(self, team_name: str) -> Optional[float]:
        import asyncio
        try:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop and loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    return pool.submit(lambda: asyncio.run(self.get_live_odd_for_team_async(team_name))).result(timeout=5.0)
            else:
                return asyncio.run(self.get_live_odd_for_team_async(team_name))
        except Exception:
            return self._get_override(team_name)

global_exchange_scraper = ExchangeScraperEngine()
