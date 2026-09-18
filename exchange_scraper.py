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

BLOCKED_KEYWORDS = [
    "t10", "simulated", "srl", "virtual", "cyber", "electronic"
]

def _fetch_fixtures_sync():
    urls = [
        "https://central.zplay1.in/pb/api/v1/events/matches/inplay",
        "https://central.zplay1.in/pb/api/v1/events/matches/4"
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=12)
            if r.status_code == 200:
                data = r.json()
                if data:
                    return data
        except Exception as e:
            logger.warning(f"[SCRAPER] Failed fetching from {url}: {e}")
    return []

async def get_live_matches():
    data = await asyncio.to_thread(_fetch_fixtures_sync)
    
    items = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("data") or data.get("events") or data.get("matches") or []

    logger.info(f"[SCRAPER] Total raw items received from central API: {len(items)}")

    real_matches = []
    seen = set()

    for item in items:
        if not isinstance(item, dict):
            continue

        # Extract name
        name = (
            item.get("name") 
            or item.get("eventName") 
            or item.get("matchName") 
            or item.get("event_name") 
            or ""
        ).strip()

        # Extract unique ID
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

    logger.info(f"[SCRAPER] Returning {len(real_matches)} valid cricket matches")
    return real_matches

def _fetch_match_details_sync(match_id: str):
    url = f"https://central.zplay1.in/pb/api/v1/events/matchDetails/{match_id}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        logger.error(f"[SCRAPER] Match details fetch error for {match_id}: {e}")
    return None

async def get_match_odds(match_id: str):
    return await asyncio.to_thread(_fetch_match_details_sync, match_id)
