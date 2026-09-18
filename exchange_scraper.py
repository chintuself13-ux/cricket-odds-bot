import asyncio
import logging
import requests
import re

logger = logging.getLogger(__name__)

EVENT_LIST_URL = "https://api.cricway.app/api/guest/event_list"
ODDS_URL = "https://odds.oramo247.com/ws/getMarketDataNew"

COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.lotus365.vip/",
    "Origin": "https://www.lotus365.vip"
}

BLOCKED_KEYWORDS = ["t10", "simulated", "srl", "virtual", "cyber", "electronic"]

DEFAULT_DOMAIN = "https://www.lotus365.vip"
CURRENT_EXCHANGE_URL = DEFAULT_DOMAIN
BASE_URL = DEFAULT_DOMAIN
BASE_EXCHANGE_URL = DEFAULT_DOMAIN
DEFAULT_CATALOG_URL = EVENT_LIST_URL
CURRENT_CATALOG_URL = EVENT_LIST_URL
DEFAULT_ODDS_URL = ODDS_URL
CURRENT_ODDS_URL = ODDS_URL

def set_exchange_url(new_url: str) -> str:
    global CURRENT_EXCHANGE_URL, BASE_URL, BASE_EXCHANGE_URL
    if new_url:
        CURRENT_EXCHANGE_URL = new_url
        BASE_URL = new_url
        BASE_EXCHANGE_URL = new_url
    return CURRENT_EXCHANGE_URL

def set_base_url(new_url: str) -> str:
    return set_exchange_url(new_url)

def set_list_url(new_url: str) -> str:
    global CURRENT_CATALOG_URL
    if new_url:
        CURRENT_CATALOG_URL = new_url
    return CURRENT_CATALOG_URL

def set_odds_url(new_url: str) -> str:
    global CURRENT_ODDS_URL
    if new_url:
        CURRENT_ODDS_URL = new_url
    return CURRENT_ODDS_URL

def clean_team_name(name: str) -> str:
    if not name:
        return ""
    name = name.lower()
    name = re.sub(r'\b(women|w|men|m|t20|odi|test|srl|xi)\b', '', name)
    name = re.sub(r'[^a-z0-9 ]', ' ', name)
    return " ".join(name.split())

def is_team_match(name1: str, name2: str) -> bool:
    c1 = clean_team_name(name1)
    c2 = clean_team_name(name2)
    if not c1 or not c2:
        return False
    if c1 == c2 or c1 in c2 or c2 in c1:
        return True
    w1 = set(c1.split())
    w2 = set(c2.split())
    return len(w1.intersection(w2)) > 0

def format_indian_odds(price):
    try:
        val = float(price)
        return f"{val:.2f}" if val > 1.0 else "0"
    except Exception:
        return str(price)

class LotusExchangeScraper:
    def __init__(self):
        self.headers = COMMON_HEADERS
        self.market_map = {}  # maps event_id -> primary market_id
        self.manual_overrides = {}

    def _clean_team_name(self, name: str) -> str:
        return clean_team_name(name)

    def set_override(self, team_name: str, odd: float):
        if team_name:
            self.manual_overrides[clean_team_name(team_name)] = float(odd)

    def set_team_odd_override(self, team_name: str, odd: float):
        self.set_override(team_name, odd)

    def clear_overrides(self):
        self.manual_overrides.clear()

    async def close_async(self):
        pass

    def _fetch_fixtures_sync(self):
        try:
            r = requests.get(EVENT_LIST_URL, headers=self.headers, timeout=12)
            logger.info(f"[SCRAPER] Event list status: {r.status_code}")
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            logger.error(f"[SCRAPER] Event list fetch error: {e}")
        return {}

    async def get_live_matches(self):
        data = await asyncio.to_thread(self._fetch_fixtures_sync)
        
        # Traverse guest event response
        raw_events = []
        if isinstance(data, dict):
            d_val = data.get("data", {})
            if isinstance(d_val, dict):
                if "events" in d_val and isinstance(d_val["events"], list):
                    raw_events.extend(d_val["events"])
                if "4" in d_val and isinstance(d_val["4"], dict):
                    for tour in d_val["4"].values():
                        if isinstance(tour, dict) and "matches" in tour:
                            raw_events.extend(tour["matches"].values() if isinstance(tour["matches"], dict) else tour["matches"])
                        elif isinstance(tour, list):
                            raw_events.extend(tour)
            elif isinstance(d_val, list):
                raw_events = d_val
            elif "events" in data:
                raw_events = data["events"]

        logger.info(f"[SCRAPER] Raw events collected: {len(raw_events)}")

        real_matches = []
        seen = set()

        for item in raw_events:
            if not isinstance(item, dict):
                continue

            # Ensure cricket sport (4) if sport/event_type ID is present
            e_type = str(item.get("event_type_id") or item.get("sport_id") or "").strip()
            if e_type and e_type != "4":
                continue

            name = (
                item.get("event_name")
                or item.get("eventName")
                or item.get("name")
                or item.get("match_name")
                or ""
            ).strip()

            e_id = str(
                item.get("event_id")
                or item.get("eventId")
                or item.get("id")
                or ""
            ).strip()

            # Capture market id
            m_id = str(
                item.get("market_id")
                or item.get("marketId")
                or (item.get("markets", [{}])[0].get("market_id") if isinstance(item.get("markets"), list) and item.get("markets") else "")
                or e_id
            ).strip()

            if not name or not e_id or e_id in seen:
                continue

            lower_name = name.lower()
            if not (" v " in lower_name or " vs " in lower_name or " - " in lower_name):
                continue

            if any(b in lower_name for b in BLOCKED_KEYWORDS):
                continue

            seen.add(e_id)
            self.market_map[e_id] = m_id
            real_matches.append({
                "id": e_id,
                "match_id": e_id,
                "match_slug": e_id,
                "name": name,
                "title": name,
                "market_id": m_id
            })

        logger.info(f"[SCRAPER] Usable live cricket matches: {len(real_matches)}")
        return real_matches

    async def fetch_live_matches_async(self):
        return await self.get_live_matches()

    def fetch_live_matches(self):
        try:
            return asyncio.run(self.get_live_matches())
        except Exception:
            return []

    def _fetch_odds_sync(self, match_id: str):
        market_id = self.market_map.get(str(match_id), str(match_id))
        payload = {"market_ids[]": market_id}
        
        try:
            r = requests.post(
                ODDS_URL,
                headers=self.headers,
                data=payload,
                timeout=10
            )
            if r.status_code == 200:
                return r.json()
            logger.warning(f"[SCRAPER] Odds fetch bad status: {r.status_code}")
        except Exception as e:
            logger.error(f"[SCRAPER] Odds request failed for {match_id}: {e}")
        return None

    async def get_match_odds(self, match_id: str):
        return await asyncio.to_thread(self._fetch_odds_sync, match_id)

    async def scrape_single_match_by_slug_async(self, match_slug: str):
        matches = await self.get_live_matches()
        for m in matches:
            if m.get("id") == match_slug or m.get("match_id") == match_slug:
                return m
        return matches[0] if matches else None

    async def _extract_odds_from_details(self, match_id: str):
        data = await self.get_match_odds(match_id)
        if not data or not isinstance(data, (dict, list)):
            return []

        items = data if isinstance(data, list) else (data.get("data") or data.get("result") or [data])
        for item in items:
            if isinstance(item, dict) and "runners" in item:
                return item["runners"]
        return []

    async def get_live_odds_data_for_match_slug_async(self, match_slug: str, team_name: str):
        c_team = clean_team_name(team_name)
        if c_team in self.manual_overrides:
            override = self.manual_overrides[c_team]
            return {
                "target_team": team_name,
                "target_odd": override,
                "target_lay": round(override + 0.05, 2),
                "opponent_team": None,
                "opponent_odd": None,
                "opponent_lay": None,
                "match_title": match_slug,
                "match_slug": match_slug,
                "match_id": match_slug,
                "is_finished": False,
                "status": "In-Play",
                "winner": None
            }

        runners = await self._extract_odds_from_details(match_slug)
        target_odd = None
        target_lay = None
        target_team = team_name
        opp_team = None
        opp_odd = None
        opp_lay = None

        if isinstance(runners, list) and len(runners) >= 2:
            for idx, r in enumerate(runners):
                if not isinstance(r, dict):
                    continue
                r_name = r.get("runnerName") or r.get("name") or r.get("nation") or ""
                if is_team_match(team_name, r_name):
                    target_team = r_name
                    target_odd = r.get("back") or r.get("b1") or r.get("price")
                    target_lay = r.get("lay") or r.get("l1")
                    other = runners[1 - idx] if len(runners) > 1 and isinstance(runners[1 - idx], dict) else {}
                    opp_team = other.get("runnerName") or other.get("name") or other.get("nation")
                    opp_odd = other.get("back") or other.get("b1") or other.get("price")
                    opp_lay = other.get("lay") or other.get("l1")
                    break

        return {
            "target_team": target_team,
            "target_odd": target_odd,
            "target_lay": target_lay,
            "opponent_team": opp_team,
            "opponent_odd": opp_odd,
            "opponent_lay": opp_lay,
            "match_title": match_slug,
            "match_slug": match_slug,
            "match_id": match_slug,
            "is_finished": False,
            "status": "In-Play",
            "winner": None
        }

    async def get_live_odds_data_for_team_async(self, team_name: str):
        matches = await self.get_live_matches()
        for m in matches:
            m_id = m.get("id") or m.get("match_id")
            if is_team_match(team_name, m.get("name", "")):
                return await self.get_live_odds_data_for_match_slug_async(m_id, team_name)

        c_team = clean_team_name(team_name)
        override = self.manual_overrides.get(c_team)
        return {
            "target_team": team_name,
            "target_odd": override,
            "target_lay": round(override + 0.05, 2) if override else None,
            "opponent_team": None,
            "opponent_odd": None,
            "opponent_lay": None,
            "match_title": None
        }

    async def get_live_odd_for_team_async(self, team_name: str):
        data = await self.get_live_odds_data_for_team_async(team_name)
        return data.get("target_odd")

    def get_live_odd_for_team(self, team_name: str):
        try:
            return asyncio.run(self.get_live_odd_for_team_async(team_name))
        except Exception:
            c_team = clean_team_name(team_name)
            return self.manual_overrides.get(c_team)

ExchangeScraper = LotusExchangeScraper
global_exchange_scraper = LotusExchangeScraper()

async def get_live_matches():
    return await global_exchange_scraper.get_live_matches()

async def get_match_odds(match_id: str):
    return await global_exchange_scraper.get_match_odds(match_id)
