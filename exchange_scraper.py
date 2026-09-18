import asyncio
import logging
import requests

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://shubhlabh777.live",
    "Referer": "https://shubhlabh777.live/"
}

BLOCKED_KEYWORDS = ["t10", "simulated", "srl", "virtual", "cyber", "electronic"]

def format_indian_odds(price):
    try:
        val = float(price)
        if val <= 1.0:
            return "0"
        return f"{val:.2f}"
    except Exception:
        return str(price)

class ExchangeScraper:
    def __init__(self):
        self.headers = HEADERS

    def _fetch_fixtures_sync(self):
        urls = [
            "https://central.zplay1.in/pb/api/v1/events/matches/inplay",
            "https://central.zplay1.in/pb/api/v1/events/matches/4"
        ]
        for url in urls:
            try:
                r = requests.get(url, headers=self.headers, timeout=12)
                if r.status_code == 200:
                    data = r.json()
                    if data:
                        return data
            except Exception as e:
                logger.warning(f"[SCRAPER] Error from {url}: {e}")
        return []

    async def get_live_matches(self):
        data = await asyncio.to_thread(self._fetch_fixtures_sync)
        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("data") or data.get("events") or data.get("matches") or []

        real_matches = []
        seen = set()

        for item in items:
            if not isinstance(item, dict):
                continue

            name = (
                item.get("name") 
                or item.get("eventName") 
                or item.get("matchName") 
                or item.get("event_name") 
                or ""
            ).strip()

            m_id = str(
                item.get("eventId") 
                or item.get("id") 
                or item.get("marketId") 
                or item.get("event_id") 
                or ""
            ).strip()

            if not name or not m_id or m_id in seen:
                continue

            lower_name = name.lower()
            if not (" v " in lower_name or " vs " in lower_name):
                continue

            if any(b in lower_name for b in BLOCKED_KEYWORDS):
                continue

            seen.add(m_id)
            real_matches.append({
                "id": m_id,
                "name": name
            })

        logger.info(f"[SCRAPER] Live cricket matches found: {len(real_matches)}")
        return real_matches

    def _fetch_odds_sync(self, match_id: str):
        url = f"https://central.zplay1.in/pb/api/v1/events/matchDetails/{match_id}"
        try:
            r = requests.get(url, headers=self.headers, timeout=12)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            logger.error(f"[SCRAPER] Match details fetch error for {match_id}: {e}")
        return None

    async def get_match_odds(self, match_id: str):
        return await asyncio.to_thread(self._fetch_odds_sync, match_id)

# Global singleton and module-level functions
global_exchange_scraper = ExchangeScraper()

async def get_live_matches():
    return await global_exchange_scraper.get_live_matches()

async def get_match_odds(match_id: str):
    return await global_exchange_scraper.get_match_odds(match_id)
