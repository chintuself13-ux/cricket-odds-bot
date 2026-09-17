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

logger = logging.getLogger("ExchangeScraper")

BASE_URL = "https://api.the-odds-api.com"

def set_base_url(new_url: str) -> str:
    global BASE_URL
    if new_url:
        BASE_URL = new_url.strip().rstrip("/")
    return BASE_URL

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
    The Odds API Engine: Fetches real-time cricket odds directly via The Odds API.
    Replaces CREX web scraping with clean, reliable API data.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self.last_fetch_time = 0
        self.last_fetch_status = 200
        self.http_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json"
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

    def _parse_odds_api_match(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(item, dict):
            return None

        match_id = str(item.get("id") or "")
        home_team = self._clean_team_name(item.get("home_team", ""))
        away_team = self._clean_team_name(item.get("away_team", ""))

        if not home_team or not away_team or home_team.lower() == away_team.lower():
            return None

        bookmakers = item.get("bookmakers", [])
        odds_arr = []

        home_back = None
        away_back = None

        if bookmakers and isinstance(bookmakers, list):
            bkm = bookmakers[0]
            markets = bkm.get("markets", [])
            if markets and isinstance(markets, list):
                mkt = markets[0]
                outcomes = mkt.get("outcomes", [])
                for outcome in outcomes:
                    o_name = str(outcome.get("name", "")).strip()
                    price = outcome.get("price")
                    if price and isinstance(price, (int, float)) and price > 1.0:
                        if self._is_strict_team_match(o_name, home_team):
                            home_back = float(price)
                        elif self._is_strict_team_match(o_name, away_team):
                            away_back = float(price)

        t1_override = self._get_override(home_team)
        t2_override = self._get_override(away_team)

        if t1_override:
            home_back = t1_override
        if t2_override:
            away_back = t2_override

        if home_back is not None and home_back > 1.0:
            odds_arr.append({
                "name": home_team,
                "back": home_back,
                "lay": round(home_back + (0.01 if home_back < 2.0 else 0.50), 2),
                "price": home_back,
                "indian_odds": format_indian_odds(home_back)
            })

        if away_back is not None and away_back > 1.0:
            odds_arr.append({
                "name": away_team,
                "back": away_back,
                "lay": round(away_back + (0.01 if away_back < 2.0 else 0.50), 2),
                "price": away_back,
                "indian_odds": format_indian_odds(away_back)
            })

        print(f"[ODDS API DEBUG] Match: {home_team} vs {away_team} | Raw Odds: {odds_arr}", flush=True)

        return {
            "id": match_id,
            "match_id": match_id,
            "match_slug": match_id,
            "title": f"{home_team} vs {away_team}",
            "sport": "Cricket (The Odds API)",
            "status": "In-Play",
            "is_finished": False,
            "winner": None,
            "home_team": home_team,
            "away_team": away_team,
            "odds": odds_arr
        }

    async def fetch_live_matches_async(self) -> List[Dict[str, Any]]:
        """
        Fetches live cricket matches exclusively via The Odds API.
        URL: https://api.the-odds-api.com/v4/sports/cricket/odds/?apiKey={ODDS_API_KEY}&regions=eu,uk&markets=h2h&oddsFormat=decimal
        """
        api_key = os.getenv("ODDS_API_KEY", "25047480db3ccb593f5c89c19de5a840").strip()
        if not api_key:
            logger.warning("ODDS_API_KEY not set in environment.")
            return []

        url = f"https://api.the-odds-api.com/v4/sports/cricket/odds/?apiKey={api_key}&regions=eu,uk&markets=h2h&oddsFormat=decimal"
        raw_data = []

        try:
            session = await self.get_aiohttp_session()
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8.0)) as resp:
                self.last_fetch_status = resp.status
                if resp.status == 200:
                    raw_data = await resp.json()
                else:
                    logger.warning(f"The Odds API returned status {resp.status}")
        except Exception as e:
            logger.warning(f"Error fetching The Odds API: {e}")
            raw_data = []

        if not raw_data:
            for s in ["cricket_odi", "cricket_test_match"]:
                try:
                    fallback_url = f"https://api.the-odds-api.com/v4/sports/{s}/odds/?apiKey={api_key}&regions=eu,uk&markets=h2h&oddsFormat=decimal"
                    session = await self.get_aiohttp_session()
                    async with session.get(fallback_url, timeout=aiohttp.ClientTimeout(total=5.0)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data:
                                raw_data = data
                                break
                except Exception:
                    pass

        matches = []
        if isinstance(raw_data, list):
            for item in raw_data:
                m_parsed = self._parse_odds_api_match(item)
                if m_parsed:
                    matches.append(m_parsed)

        return matches

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
                    return pool.submit(lambda: asyncio.run(self.fetch_live_matches_async())).result(timeout=10.0)
            else:
                return asyncio.run(self.fetch_live_matches_async())
        except Exception as e:
            logger.warning(f"Error in sync fetch_live_matches: {e}")
            return []

    async def scrape_single_match_by_slug_async(self, slug: str) -> Optional[Dict[str, Any]]:
        matches = await self.fetch_live_matches_async()
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
        matches = await self.fetch_live_matches_async()

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

        matches = await self.fetch_live_matches_async()
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
