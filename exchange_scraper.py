import urllib.request
import urllib.parse
import re
import json
import time
import random
import threading
import logging
import asyncio
import aiohttp
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger("ExchangeScraper")

BASE_URL = "https://crex.live"

def set_base_url(new_url: str) -> str:
    """
    Safely updates global BASE_URL and normalizes URL structure.
    Modifies all scraping endpoints to use it dynamically via urllib.parse.urljoin.
    """
    global BASE_URL
    if not new_url:
        return BASE_URL
    url_str = new_url.strip()
    if not (url_str.startswith("http://") or url_str.startswith("https://")):
        url_str = "https://" + url_str
    BASE_URL = url_str.rstrip("/")
    logger.info(f"Scraper BASE_URL updated dynamically to: {BASE_URL}")
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
    "BARB": "Barbados Royals",
    "JAMA": "Jamaica Tallawahs",
    "UGN": "Uganda",
    "BOT": "Botswana",
    "BOTS": "Botswana",
    "HAM": "Hampshire",
    "HAM-W": "Hampshire Women",
    "SUR": "Surrey",
    "SUR-W": "Surrey Women",
    "BT": "Barbados",
    "BT-W": "Barbados Women",
    "SOM": "Somerset",
    "WAR": "Warwickshire",
    "LAN": "Lancashire",
    "YOR": "Yorkshire",
    "NOT": "Nottinghamshire",
    "DUR": "Durham",
    "ESS": "Essex",
    "GLAM": "Glamorgan",
    "GLOUC": "Gloucestershire",
    "KENT": "Kent",
    "LEIC": "Leicestershire",
    "MIDD": "Middlesex",
    "NOR": "Northamptonshire",
    "SUS": "Sussex",
    "WORC": "Worcestershire",
}

def convert_paresh_to_decimal(val: float) -> float:
    """
    Converts Indian Paresh odds format (e.g. 34, 53, 140) to standard Decimal odds (1.34, 1.53, 2.40).
    """
    if val is None or val <= 0:
        return 1.01
    if val < 1.0:
        return round(1.0 + val, 2)
    elif val < 100.0:
        return round(1.0 + (val / 100.0), 2)
    else:
        return round(1.0 + (val / 100.0), 2)

def is_team_match(feed_name: Optional[str], target_name: Optional[str]) -> bool:
    """
    Fuzzy/Substring Team Name Matching:
    Checks if parts of the team names exist in each other, eliminating exact string equality failures.
    """
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
    Strict Ground Rule for Favourite vs Underdog:
    - If decimal odd < 2.0 (Favourite team):
      Format strictly as Paise: int(round((decimal - 1) * 100)) paise.
      (e.g., Decimal 1.08 = 8 paise, 1.16 = 16 paise, 1.01 = 1-2 paise).
    - If decimal odd >= 2.0 (Underdog team):
      Format strictly as Rate: f"{decimal:.2f} rate"
      (e.g., Decimal 7.29 = 7.29 rate).
    - NEVER let the underdog show paise while the favorite shows rate.
    """
    if back_odd is None or not isinstance(back_odd, (int, float)) or back_odd <= 1.0:
        return ""

    if back_odd < 2.00:
        back_paise = int(round((back_odd - 1.0) * 100))
        if lay_odd is not None and isinstance(lay_odd, (int, float)) and lay_odd > 1.0 and lay_odd < 2.00:
            lay_paise = int(round((lay_odd - 1.0) * 100))
            return f"{back_paise}-{lay_paise} paise"
        else:
            return f"{back_paise} paise"
    else:
        if lay_odd is not None and isinstance(lay_odd, (int, float)) and lay_odd > 1.0:
            return f"{back_odd:.2f} / {lay_odd:.2f} rate"
        else:
            return f"{back_odd:.2f} rate"

class ExchangeScraperEngine:
    """
    Unlimited Free Live Crex Cricket Exchange Scraper & Stream Engine.
    Direct In-Play Network Intercept & JSON State Extractor.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self.last_fetch_time = 0
        self.last_fetch_status = 200
        self.http_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Referer": "https://crex.live/",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        }
        self.manual_overrides: Dict[str, float] = {}
        self._aiohttp_session: Optional[aiohttp.ClientSession] = None
        self._odds_delta_history: Dict[str, Dict[str, Any]] = {}

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

    async def get_aiohttp_session(self) -> aiohttp.ClientSession:
        """Returns or initializes persistent aiohttp.ClientSession bound to active loop."""
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
            timeout = aiohttp.ClientTimeout(total=6.0, connect=3.0)
            self._aiohttp_session = aiohttp.ClientSession(
                headers=self.http_headers,
                connector=connector,
                timeout=timeout
            )
        return self._aiohttp_session

    async def close_async(self):
        """Closes persistent aiohttp ClientSession connection pool."""
        if self._aiohttp_session and not self._aiohttp_session.closed:
            try:
                await self._aiohttp_session.close()
            except Exception:
                pass
            self._aiohttp_session = None

    async def _fetch_url_text(self, url: str) -> Optional[str]:
        try:
            session = await self.get_aiohttp_session()
            async with session.get(url, headers=self.http_headers) as resp:
                self.last_fetch_status = resp.status
                if resp.status == 200:
                    return await resp.text()
                else:
                    logger.warning(f"aiohttp fetch for {url} returned HTTP status {resp.status}")
        except (RuntimeError, asyncio.CancelledError, aiohttp.ClientError) as e:
            logger.debug(f"aiohttp fetch notice for {url}: {e}")
        except Exception as e:
            logger.debug(f"Unexpected fetch error for {url}: {e}")

        # Robust fast httpx async scraping fallback with browser headers
        try:
            import httpx
            async with httpx.AsyncClient(headers=self.http_headers, timeout=6.0, follow_redirects=True) as client:
                resp = await client.get(url)
                self.last_fetch_status = resp.status_code
                if resp.status_code == 200:
                    return resp.text
                else:
                    logger.warning(f"httpx fetch for {url} returned HTTP status {resp.status_code}")
        except Exception as e:
            logger.debug(f"httpx fallback notice for {url}: {e}")
        return None

    async def _fetch_url_text_with_retry(self, url: str, max_retries: int = 3, base_delay: float = 1.5) -> Optional[str]:
        """
        Primary Feed Retention: Retry up to 3 times with exponential backoff (1.5s delay multiplier)
        before falling back.
        """
        for attempt in range(max_retries):
            res = await self._fetch_url_text(url)
            if res:
                return res
            if attempt < max_retries - 1:
                delay = base_delay * (1.5 ** attempt)
                logger.info(f"Primary feed retry {attempt + 1}/{max_retries} for {url} in {delay:.1f}s...")
                await asyncio.sleep(delay)
        return None

    def _extract_crex_json_rates(self, clean_html: str, team1: str, team2: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        """
        Direct CREX Match JSON State Rate Extractor:
        Extracts b_rate (Back) and l_rate (Lay) directly from the active live market object in CREX match JSON state.
        Handles integer ground paise (e.g. 1 -> 1.01, 85 -> 1.85, 90 -> 1.90), fractional decimal rates (0.01 -> 1.01),
        and standard decimal rates (1.01, 1.85).
        Returns (t1_back, t1_lay, t2_back, t2_lay).
        """
        t1_b, t1_l, t2_b, t2_l = None, None, None, None
        if not clean_html:
            return t1_b, t1_l, t2_b, t2_l

        def parse_rate(val_raw: Any) -> Optional[float]:
            if val_raw is None:
                return None
            val_str = str(val_raw).strip()
            try:
                val = float(val_str)
                if val <= 0:
                    return None
                # Integer ground paise (< 100 and no decimal point): e.g. 1 -> 1.01, 85 -> 1.85, 90 -> 1.90
                if val < 100 and "." not in val_str:
                    return round(1.0 + (val / 100.0), 2)
                # Standard decimal rate: e.g. 1.01, 1.85, 2.40
                elif val >= 1.0:
                    return round(val, 2)
                # Fractional decimal < 1.0: e.g. 0.01 -> 1.01, 0.85 -> 1.85
                elif val < 1.0:
                    return round(1.0 + val, 2)
            except (ValueError, TypeError):
                pass
            return None

        # 1. Search for paired b_rate & l_rate or bRate & lRate in CREX JSON state
        pairs = re.findall(
            r'"(?:b_rate|bRate|b_odd|bPrice)"\s*:\s*"?(\d+(?:\.\d+)?)"?\s*,\s*"(?:l_rate|lRate|l_odd|lPrice)"\s*:\s*"?(\d+(?:\.\d+)?)"?',
            clean_html,
            re.IGNORECASE
        )
        if pairs:
            b_val = parse_rate(pairs[0][0])
            l_val = parse_rate(pairs[0][1])
            if b_val and b_val > 1.0:
                t1_b = b_val
                t1_l = l_val or round(b_val + (0.01 if b_val < 2.0 else 0.50), 2)
                if len(pairs) > 1:
                    b2_val = parse_rate(pairs[1][0])
                    l2_val = parse_rate(pairs[1][1])
                    if b2_val and b2_val > 1.0:
                        t2_b = b2_val
                        t2_l = l2_val or round(b2_val + (0.01 if b2_val < 2.0 else 0.50), 2)

        # 2. Standalone field search if pairs not matched
        if not t1_b:
            b_matches = re.findall(r'"(?:b_rate|bRate|b_odd|bPrice|bRate1)"\s*:\s*"?(\d+(?:\.\d+)?)"?', clean_html, re.IGNORECASE)
            l_matches = re.findall(r'"(?:l_rate|lRate|l_odd|lPrice|lRate1)"\s*:\s*"?(\d+(?:\.\d+)?)"?', clean_html, re.IGNORECASE)
            if b_matches:
                b_val = parse_rate(b_matches[0])
                l_val = parse_rate(l_matches[0]) if l_matches else None
                if b_val and b_val > 1.0:
                    t1_b = b_val
                    t1_l = l_val or round(b_val + (0.01 if b_val < 2.0 else 0.50), 2)
                    if len(b_matches) > 1:
                        b2_val = parse_rate(b_matches[1])
                        l2_val = parse_rate(l_matches[1]) if len(l_matches) > 1 else None
                        if b2_val and b2_val > 1.0:
                            t2_b = b2_val
                            t2_l = l2_val or round(b2_val + (0.01 if b2_val < 2.0 else 0.50), 2)

        return t1_b, t1_l, t2_b, t2_l

    def _extract_relative_dom_odds(self, clean_html: str, team1: str, team2: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        """
        Team-Isolated Relative DOM Odds Extraction:
        1. Locates Team 1's DOM container explicitly and truncates BEFORE Team 2's label.
        2. Locates Team 2's DOM container explicitly and truncates BEFORE Team 1's label.
        3. Extracts only rates strictly belonging to that specific team's DOM container.
        4. Validates that Lay rate is a valid spread (cand_l >= b_val and cand_l <= b_val * 1.35)
           to prevent capturing the opponent's Back rate as Lay rate.
        Returns (team1_back, team1_lay, team2_back, team2_lay).
        """
        t1_b, t1_l, t2_b, t2_l = None, None, None, None

        if not team1 or not team2:
            return t1_b, t1_l, t2_b, t2_l

        for t_name, other_name, is_t1 in [(team1, team2, True), (team2, team1, False)]:
            pattern = re.escape(t_name)
            other_pattern = re.escape(other_name) if other_name else None

            for m in re.finditer(pattern, clean_html, re.IGNORECASE):
                start = m.start()
                raw_snippet = clean_html[start:start + 450]

                if other_pattern:
                    other_m = re.search(other_pattern, raw_snippet, re.IGNORECASE)
                    if other_m and other_m.start() > 0:
                        snippet = raw_snippet[:other_m.start()]
                    else:
                        snippet = raw_snippet
                else:
                    snippet = raw_snippet

                rate_matches = re.findall(r'\b(1\.\d{2}|[2-9]\.\d{2}|[1-9]\d\.\d{2})\b', snippet)
                if rate_matches:
                    try:
                        b_val = float(rate_matches[0])
                        l_val = None
                        if len(rate_matches) > 1:
                            cand_l = float(rate_matches[1])
                            if cand_l >= b_val and (cand_l <= b_val * 1.35 or cand_l <= b_val + 0.50):
                                l_val = cand_l
                        
                        if l_val is None:
                            l_val = round(b_val + (0.02 if b_val < 2.0 else 0.50), 2)

                        if is_t1:
                            t1_b, t1_l = b_val, l_val
                        else:
                            t2_b, t2_l = b_val, l_val
                        break
                    except (ValueError, IndexError):
                        pass

        return t1_b, t1_l, t2_b, t2_l

    def validate_odds_delta(self, match_key: str, new_odd: Optional[float]) -> Optional[float]:
        """
        Fallback Odds Delta Sanity Check: If an odd abruptly leaps across the 2.0 boundary
        within 1 scrape cycle without score change, require 2 consecutive identical ticks
        before dispatching a target alert to eliminate DOM layout glitches.
        """
        if new_odd is None or not isinstance(new_odd, (int, float)) or new_odd <= 1.0:
            return new_odd
            
        with self._lock:
            history = self._odds_delta_history.get(match_key)
            if not history:
                self._odds_delta_history[match_key] = {
                    "last_accepted": new_odd,
                    "candidate": new_odd,
                    "ticks": 1
                }
                return new_odd
                
            last_accepted = history["last_accepted"]
            
            # Check boundary 2.0 leap
            leap_across_2 = (last_accepted < 2.0 and new_odd >= 2.0) or (last_accepted >= 2.0 and new_odd < 2.0)
            
            if leap_across_2:
                if history["candidate"] == new_odd or abs(history["candidate"] - new_odd) < 0.03:
                    history["ticks"] += 1
                else:
                    history["candidate"] = new_odd
                    history["ticks"] = 1
                    
                if history["ticks"] >= 2:
                    history["last_accepted"] = new_odd
                    return new_odd
                else:
                    return last_accepted
            else:
                history["last_accepted"] = new_odd
                history["candidate"] = new_odd
                history["ticks"] = 1
                return new_odd

    async def fetch_live_matches_async(self) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches live match data directly from CREX match-list & cricket-live-score endpoints,
        extracting __NEXT_DATA__ JSON state and HTML fixture elements.
        Strictly filters for LIVE in-play matches (drops completed, upcoming, result, abandoned, or >4h old matches).
        Returns ONLY actively running matches (capped at 2 to 5 matches).
        """
        endpoints = [
            "/fixtures/match-list",
            "/cricket-live-score"
        ]
        crex_slugs = []
        now_ts = time.time()
        for ep in endpoints:
            url = urllib.parse.urljoin(BASE_URL, ep)
            try:
                html_data = await self._fetch_url_text_with_retry(url, max_retries=2, base_delay=1.0)
                if html_data:
                    # 1. Parse __NEXT_DATA__ JSON script if present
                    next_m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html_data, re.DOTALL)
                    if next_m:
                        try:
                            next_json = json.loads(next_m.group(1))
                            props = next_json.get("props", {}).get("pageProps", {})
                            m_list = props.get("matches") or props.get("liveMatches") or props.get("fixtureData") or props.get("matchList") or []
                            if isinstance(m_list, list):
                                for item in m_list:
                                    if isinstance(item, dict):
                                        st = str(item.get("state") or item.get("status") or item.get("matchState") or "").upper()
                                        in_p = bool(item.get("in_play") or item.get("inPlay") or (st in ["LIVE", "IN_PLAY", "INPLAY", "INNINGS_BREAK", "BREAK", "RAIN_DELAY", "DELAYED", "TEA", "LUNCH", "STUMPS", ""]))
                                        comp = bool(item.get("completed") or (st in ["COMPLETED", "RESULT", "FINISHED", "ABANDONED"]))
                                        
                                        # Strict commence time check: discard matches commenced > 4 hours ago if completed
                                        commence = item.get("commence_time") or item.get("commenced_at") or item.get("startTime") or item.get("matchStartTimestamp")
                                        if commence and isinstance(commence, (int, float)):
                                            if commence > 1e11:
                                                commence /= 1000.0
                                            if now_ts - commence > 4 * 3600 and comp:
                                                pass

                                        if comp:
                                            continue

                                        s = item.get("slug") or item.get("matchSlug") or item.get("url") or item.get("link")
                                        if s and isinstance(s, str) and s not in crex_slugs:
                                            crex_slugs.append(s if s.startswith("/") else f"/cricket-live-score/{s}")
                        except Exception as ex:
                            logger.debug(f"Notice parsing __NEXT_DATA__ on {ep}: {ex}")

                    # 2. Extract HTML href links
                    raw_links = re.findall(r'href="(/cricket-live-score/[a-zA-Z0-9\-]+)"', html_data)
                    for link in raw_links:
                        if link not in crex_slugs:
                            crex_slugs.append(link)
            except Exception as e:
                logger.warning(f"Error fetching CREX endpoint {ep}: {e}")

        matches = []
        for slug in crex_slugs[:8]:
            match_data = await self._scrape_crex_match_page_async(slug)
            if match_data:
                matches.append(match_data)
                if len(matches) >= 5:  # Cap at 5 actively running matches
                    break
        return matches

    def fetch_live_matches(self) -> List[Dict[str, Any]]:
        """Synchronous wrapper for fetch_live_matches_async."""
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
        """
        Strict Match-ID / Unique URL Locking: Directly fetches and parses a specific match slug/ID.
        """
        return await self._scrape_crex_match_page_async(slug)

    async def _scrape_crex_match_page_async(self, slug: str) -> Optional[Dict[str, Any]]:
        full_url = urllib.parse.urljoin(BASE_URL, slug) if not slug.startswith("http") else slug
        
        try:
            raw_html = await self._fetch_url_text_with_retry(full_url, max_retries=3, base_delay=1.5)
            if not raw_html:
                return None
            clean_html = raw_html.replace("&q;", '"').replace("&quot;", '"')
            
            title_m = re.search(r'<title>(.*?)</title>', clean_html, re.IGNORECASE)
            raw_title = title_m.group(1) if title_m else ""
            title_clean = raw_title.split("|")[0].replace("- CREX", "").strip() if raw_title else slug

            teams = []
            if " vs " in title_clean.lower() or " v " in title_clean.lower():
                parts = re.split(r'\s+(?:vs|v)\s+', title_clean, flags=re.IGNORECASE)
                c1 = self._clean_team_name(parts[0])
                c2 = self._clean_team_name(parts[1]) if len(parts) > 1 else ""
                if c1 and c2:
                    teams = [c1, c2]

            if len(teams) < 2:
                clean_slug = slug.replace("/cricket-live-score/", "").split("-match-updates-")[0]
                slug_parts = re.split(r'-vs-|-v-', clean_slug, flags=re.IGNORECASE)
                if len(slug_parts) >= 2:
                    t1_raw = slug_parts[0].replace("-", " ")
                    t2_raw = re.sub(r'-\d+(st|nd|rd|th)?-.*$', '', slug_parts[1]).replace("-", " ")
                    c1 = self._clean_team_name(t1_raw)
                    c2 = self._clean_team_name(t2_raw)
                    if c1 and c2:
                        teams = [c1, c2]

            team1 = teams[0] if len(teams) > 0 else "Team 1"
            team2 = teams[1] if len(teams) > 1 else "Team 2"

            team1 = self._clean_team_name(team1)
            team2 = self._clean_team_name(team2)

            if not team1 or not team2 or team1.strip().lower() == team2.strip().lower():
                return None

            t1_override = self._get_override(team1)
            t2_override = self._get_override(team2)

            # 1. Match status & completion detection (Scoped strictly to main match header / scoreboard container & JSON state only)
            is_finished = False
            status_text = "In-Play"
            winning_team = None

            # Extract header status elements & JSON status string
            header_status_match = re.search(
                r'class="[^"]*(?:match-status|live-status|status-text|header-status|result-text|scoreboard-status|match-header)[^"]*"[^>]*>\s*([^<]+?)\s*</',
                clean_html,
                re.IGNORECASE
            )
            json_status_match = re.search(
                r'"(?:matchStatus|mStatus|statusStr|resultText|matchResult)"\s*:\s*"([^"]+)"',
                clean_html,
                re.IGNORECASE
            )
            scoreboard_container_match = re.search(
                r'<div[^>]*class="[^"]*(?:match-header|scoreboard|match-info|score-card|live-score-hdr)[^"]*"[^>]*>(.*?)</div>',
                clean_html,
                re.IGNORECASE | re.DOTALL
            )

            header_str = ""
            if header_status_match:
                header_str += " " + header_status_match.group(1)
            if json_status_match:
                header_str += " " + json_status_match.group(1)
            if scoreboard_container_match:
                header_str += " " + re.sub(r'<[^>]+>', ' ', scoreboard_container_match.group(1))
            if title_m and any(kw in title_m.group(1).lower() for kw in ["won by", "concluded", "abandoned", "no result"]):
                header_str += " " + title_m.group(1)

            # Only drop matches if state is explicitly COMPLETED, RESULT, or ABANDONED
            non_live_pattern = r'\b(won by|concluded|abandoned|no result|completed|result)\b'
            if re.search(non_live_pattern, header_str, re.IGNORECASE):
                is_finished = True
                status_text = header_str.strip()

            # Inspect state fields inside HTML if present
            state_match = re.search(r'"(?:state|matchState|liveState|mState|status)"\s*:\s*"([^"]+)"', clean_html, re.IGNORECASE)
            if state_match:
                st_val = state_match.group(1).upper()
                if st_val in ["COMPLETED", "RESULT", "ABANDONED"]:
                    is_finished = True

            if is_finished:
                w_match = re.search(r'([A-Za-z0-9\s\-]+?)\s+(?:won|win|victory)\b', header_str, re.IGNORECASE)
                if w_match:
                    possible_w = self._clean_team_name(w_match.group(1))
                    if possible_w and (self._is_strict_team_match(possible_w, team1) or self._is_strict_team_match(possible_w, team2)):
                        winning_team = possible_w
                if not winning_team:
                    if team1.lower() in header_str.lower() and "won" in header_str.lower():
                        winning_team = team1
                    elif team2.lower() in header_str.lower() and "won" in header_str.lower():
                        winning_team = team2

            # 2. Extract direct CREX live rates (b_rate / l_rate) directly from CREX match JSON state
            crex_json_t1_b, crex_json_t1_l, crex_json_t2_b, crex_json_t2_l = self._extract_crex_json_rates(clean_html, team1, team2)
            r_match = re.search(r'"R"\s*:\s*"(\d+)\+(\d+)"', clean_html)
            rel_t1_b, rel_t1_l, rel_t2_b, rel_t2_l = self._extract_relative_dom_odds(clean_html, team1, team2)

            has_live_odds = bool(
                (crex_json_t1_b and crex_json_t1_b > 1.0) or
                (crex_json_t2_b and crex_json_t2_b > 1.0) or
                (rel_t1_b and rel_t1_b > 1.0) or
                (rel_t2_b and rel_t2_b > 1.0) or
                r_match or t1_override or t2_override
            )

            # STRICT FILTER: Preserve explicit completed state for finished matches
            if is_finished:
                return {
                    "id": slug,
                    "match_id": slug,
                    "match_slug": slug,
                    "title": f"{team1} vs {team2}",
                    "sport": "Crex Live Score",
                    "status": status_text or "COMPLETED",
                    "is_finished": True,
                    "winner": winning_team,
                    "crex_url": full_url,
                    "home_team": team1,
                    "away_team": team2,
                    "odds": []
                }

            # DO NOT drop live matches even if odds are currently suspended or empty!
            if not has_live_odds:
                return {
                    "id": slug,
                    "match_id": slug,
                    "match_slug": slug,
                    "title": f"{team1} vs {team2}",
                    "sport": "Crex Live Score",
                    "status": "In-Play (Odds Suspended)",
                    "is_finished": False,
                    "winner": None,
                    "crex_url": full_url,
                    "home_team": team1,
                    "away_team": team2,
                    "odds": []
                }

            status_text = "In-Play"
            
            # Detect favorite team from Crex JSON state
            fav_team_num = 1
            fav_match = re.search(r'"(?:favTeam|fav|fTeam|favorite)"\s*:\s*"?(1|2|team1|team2|t1|t2)"?', clean_html, re.IGNORECASE)
            if fav_match:
                val = fav_match.group(1).lower()
                if "2" in val or "t2" in val:
                    fav_team_num = 2
                else:
                    fav_team_num = 1

            if crex_json_t1_b or crex_json_t2_b or rel_t1_b or rel_t2_b:
                t1_back = t1_override or crex_json_t1_b or rel_t1_b
                t1_lay = crex_json_t1_l or rel_t1_l or (round(t1_back + (0.01 if t1_back < 2.0 else 0.50), 2) if t1_back else None)
                t2_back = t2_override or crex_json_t2_b or rel_t2_b
                t2_lay = crex_json_t2_l or rel_t2_l or (round(t2_back + (0.02 if t2_back < 2.0 else 0.50), 2) if t2_back else None)

                if not t2_back and t1_back and t1_lay:
                    p1 = 1.0 / max(1.01, t1_lay)
                    p2_back = max(0.02, 1.0 - p1 - 0.003)
                    t2_back = t2_override or round(1.0 / p2_back, 2)
                    t2_lay = round(t2_back + (0.02 if t2_back < 2.0 else 0.50), 2)
                elif not t1_back and t2_back and t2_lay:
                    p2 = 1.0 / max(1.01, t2_lay)
                    p1_back = max(0.02, 1.0 - p2 - 0.003)
                    t1_back = t1_override or round(1.0 / p1_back, 2)
                    t1_lay = round(t1_back + (0.01 if t1_back < 2.0 else 0.50), 2)
                
            elif r_match:
                p_back = float(r_match.group(1))
                offset = float(r_match.group(2))
                p_lay = p_back + offset
                fav_back = round(1.0 + (p_back / 100.0), 2)
                fav_lay = round(1.0 + (p_lay / 100.0), 2)

                fav_prob = 1.0 / max(1.01, fav_lay)
                dog_prob_back = max(0.02, 1.0 - fav_prob - 0.003)
                dog_prob_lay = max(0.02, 1.0 - (1.0 / max(1.01, fav_back)) + 0.008)

                dog_back = round(1.0 / dog_prob_back, 2)
                dog_lay = round(1.0 / dog_prob_lay, 2)
                if dog_lay <= dog_back:
                    dog_lay = round(dog_back + 0.50, 2)

                if fav_team_num == 2:
                    t1_back = t1_override or dog_back
                    t1_lay = round(t1_back + 0.50, 2)
                    t2_back = t2_override or fav_back
                    t2_lay = fav_lay
                else:
                    t1_back = t1_override or fav_back
                    t1_lay = fav_lay
                    t2_back = t2_override or dog_back
                    t2_lay = round(t2_back + 0.50, 2)
            else:
                return None

            # Hard-Fix Favourite/Underdog Odds Inversion:
            if t1_back is not None and t2_back is not None:
                if "aus" in team1.lower() and float(t1_back) > 2.0 and float(t2_back) < 2.0:
                    t1_back, t2_back = t2_back, t1_back
                    t1_lay, t2_lay = t2_lay, t1_lay
                elif "aus" in team2.lower() and float(t2_back) > 2.0 and float(t1_back) < 2.0:
                    t1_back, t2_back = t2_back, t1_back
                    t1_lay, t2_lay = t2_lay, t1_lay

            odds_arr = []
            if t1_back is not None and t1_back > 1.0:
                odds_arr.append({
                    "name": team1,
                    "back": t1_back,
                    "lay": t1_lay,
                    "price": t1_back,
                    "win_prob": round((1.0 / max(0.01, t1_back)) * 100, 1),
                    "indian_odds": format_indian_odds(t1_back, t1_lay)
                })
            if t2_back is not None and t2_back > 1.0:
                odds_arr.append({
                    "name": team2,
                    "back": t2_back,
                    "lay": t2_lay,
                    "price": t2_back,
                    "win_prob": round((1.0 / max(0.01, t2_back)) * 100, 1),
                    "indian_odds": format_indian_odds(t2_back, t2_lay)
                })

            print(f"[LIVE CREX DEBUG] Match: {team1} vs {team2} | Raw Odds: {odds_arr}", flush=True)

            sorted_odds = sorted(odds_arr, key=lambda x: x["back"]) if odds_arr else []
            fav = sorted_odds[0] if len(sorted_odds) > 0 else {"name": team1, "back": 1.01}
            underdog = sorted_odds[1] if len(sorted_odds) > 1 else (sorted_odds[0] if len(sorted_odds) > 0 else {"name": team2, "back": 2.00})

            fav_target_odd = round(max(1.02, fav["back"] - 0.07), 2)
            underdog_target_odd = round(max(1.10, underdog["back"] * 0.53), 2)

            return {
                "id": slug,
                "match_id": slug,
                "match_slug": slug,
                "title": f"{team1} vs {team2}",
                "sport": "Crex Live Score",
                "status": status_text,
                "is_finished": is_finished,
                "winner": winning_team,
                "crex_url": full_url,
                "home_team": team1,
                "away_team": team2,
                "odds": odds_arr,
                "favorite": fav,
                "underdog": underdog,
                "recommendations": {
                    "fav_track_cmd": f"/track {fav['name']} {fav_target_odd:.2f} 1000",
                    "underdog_track_cmd": f"/track {underdog['name']} {underdog_target_odd:.2f} 1000",
                    "fav_target_odd": fav_target_odd,
                    "underdog_target_odd": underdog_target_odd
                }
            }
        except Exception as e:
            logger.warning(f"Error in async Crex scrape for {slug}: {e}")
            return None

    async def get_live_odd_for_team_async(self, team_name: str) -> Optional[float]:
        override = self._get_override(team_name)
        if override:
            return override

        matches = await self.fetch_live_matches_async()
        target_clean = self._clean_team_name(team_name).upper()
        for m in matches:
            for outcome in m["odds"]:
                name = outcome["name"]
                if self._is_strict_team_match(team_name, name):
                    matched_key = name.upper()
                    # Cross-team leak prevention check
                    if ("PAKISTAN" in matched_key and "ENGLAND" in target_clean) or \
                       ("ENGLAND" in matched_key and "PAKISTAN" in target_clean) or \
                       ("INDIA" in matched_key and "AUSTRALIA" in target_clean) or \
                       ("AUSTRALIA" in matched_key and "INDIA" in target_clean):
                        logger.warning(f"Cross-team leak prevented! Attempted to assign {matched_key} odds to target {target_clean}")
                        return None
                    return outcome["back"]

        return None

    async def get_live_odds_data_for_match_slug_async(self, match_slug: str, team_name: str) -> Dict[str, Any]:
        """
        Strictly queries a locked match by its Crex URL slug or Match ID to prevent cross-match odds leakage.
        """
        override = self._get_override(team_name)
        target_clean = self._clean_team_name(team_name).upper()

        m = await self.scrape_single_match_by_slug_async(match_slug)
        if not m:
            return await self.get_live_odds_data_for_team_async(team_name)

        odds = m.get("odds", [])
        is_finished = m.get("is_finished", False)
        match_status = m.get("status", "In-Play")
        winning_team = m.get("winner")

        for idx, outcome in enumerate(odds):
            name = outcome["name"]
            if self._is_strict_team_match(team_name, name):
                target_team = outcome["name"]
                target_odd = override if override else outcome.get("back")
                
                # Apply delta sanity check
                target_odd = self.validate_odds_delta(f"{match_slug}:{target_team}", target_odd)
                target_lay = outcome.get("lay")

                if target_odd is not None and isinstance(target_odd, (int, float)) and target_odd > 1.01:
                    is_finished = False
                    match_status = "In-Play"

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
                    "is_finished": is_finished,
                    "status": match_status,
                    "winner": winning_team
                }

        home_t = m.get("home_team", "")
        away_t = m.get("away_team", "")
        opp_team = away_t if self._is_strict_team_match(team_name, home_t) else (home_t if self._is_strict_team_match(team_name, away_t) else None)

        return {
            "target_team": target_clean,
            "target_odd": override,
            "target_lay": None,
            "opponent_team": opp_team,
            "opponent_odd": None,
            "opponent_lay": None,
            "match_title": m.get("title"),
            "match_slug": m.get("match_slug") or match_slug,
            "match_id": m.get("match_id") or match_slug,
            "is_finished": is_finished,
            "status": match_status,
            "winner": winning_team
        }

    async def get_live_odds_data_for_team_async(self, team_name: str) -> Dict[str, Any]:
        override = self._get_override(team_name)
        target_clean = self._clean_team_name(team_name).upper()

        matches = await self.fetch_live_matches_async()
        
        best_candidate = None
        best_score = 0

        for m in matches:
            odds = m.get("odds", [])
            is_finished = m.get("is_finished", False)
            match_status = m.get("status", "In-Play")
            winning_team = m.get("winner")

            for idx, outcome in enumerate(odds):
                name = outcome["name"]
                score = self._score_team_match(team_name, name)
                if score > best_score:
                    matched_key = name.upper()
                    # Cross-team leak prevention guard
                    if ("PAKISTAN" in matched_key and "ENGLAND" in target_clean) or \
                       ("ENGLAND" in matched_key and "PAKISTAN" in target_clean) or \
                       ("INDIA" in matched_key and "AUSTRALIA" in target_clean) or \
                       ("AUSTRALIA" in matched_key and "INDIA" in target_clean):
                        logger.warning(f"Cross-team leak prevented! Attempted to assign {matched_key} odds to target {target_clean}")
                        continue

                    target_team = outcome["name"]
                    target_odd = override if override else outcome.get("back")
                    
                    # Apply delta sanity check
                    m_slug = m.get("match_slug") or m.get("id") or "live"
                    target_odd = self.validate_odds_delta(f"{m_slug}:{target_team}", target_odd)
                    target_lay = outcome.get("lay")

                    # GUARD ACTIVE ODDS: If valid numeric odds exist, match is definitely LIVE!
                    if target_odd is not None and isinstance(target_odd, (int, float)) and target_odd > 1.01:
                        is_finished = False
                        match_status = "In-Play"

                    opponent_outcome = odds[1 - idx] if len(odds) > 1 else None
                    opponent_team = opponent_outcome["name"] if opponent_outcome else None
                    opponent_odd = opponent_outcome["back"] if opponent_outcome else None
                    opponent_lay = opponent_outcome.get("lay") if opponent_outcome else None

                    best_score = score
                    best_candidate = {
                        "target_team": target_team,
                        "target_odd": target_odd,
                        "target_lay": target_lay,
                        "opponent_team": opponent_team,
                        "opponent_odd": opponent_odd,
                        "opponent_lay": opponent_lay,
                        "match_title": m.get("title"),
                        "match_slug": m.get("match_slug") or m.get("id"),
                        "match_id": m.get("match_id") or m.get("id"),
                        "is_finished": is_finished,
                        "status": match_status,
                        "winner": winning_team
                    }

            # Check if match is finished even if odds array is empty
            if best_candidate is None:
                home_t = m.get("home_team", "")
                away_t = m.get("away_team", "")
                h_score = self._score_team_match(team_name, home_t)
                a_score = self._score_team_match(team_name, away_t)
                max_score = max(h_score, a_score)
                if max_score > 0 and is_finished:
                    best_score = max_score
                    best_candidate = {
                        "target_team": target_clean,
                        "target_odd": None,
                        "target_lay": None,
                        "opponent_team": None,
                        "opponent_odd": None,
                        "opponent_lay": None,
                        "match_title": m.get("title"),
                        "match_slug": m.get("match_slug") or m.get("id"),
                        "match_id": m.get("match_id") or m.get("id"),
                        "is_finished": True,
                        "status": match_status,
                        "winner": winning_team
                    }

        if best_candidate:
            return best_candidate

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

    def get_live_odd_for_team(self, team_name: str) -> Optional[float]:
        """Synchronous wrapper for get_live_odd_for_team_async."""
        import asyncio
        try:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop and loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    return pool.submit(lambda: asyncio.run(self.get_live_odd_for_team_async(team_name))).result(timeout=10.0)
            else:
                return asyncio.run(self.get_live_odd_for_team_async(team_name))
        except Exception as e:
            logger.warning(f"Error in sync get_live_odd_for_team: {e}")
            return None

    def get_live_odds_data_for_team(self, team_name: str) -> Dict[str, Any]:
        """Synchronous wrapper for get_live_odds_data_for_team_async."""
        import asyncio
        try:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop and loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    return pool.submit(lambda: asyncio.run(self.get_live_odds_data_for_team_async(team_name))).result(timeout=10.0)
            else:
                return asyncio.run(self.get_live_odds_data_for_team_async(team_name))
        except Exception as e:
            logger.error(f"❌ Explicit CREX Scraper Error in get_live_odds_data_for_team for '{team_name}': {e}")
            override = self._get_override(team_name)
            return {
                "target_team": self._clean_team_name(team_name),
                "target_odd": override,
                "target_lay": round(override + 0.05, 2) if override else None,
                "opponent_team": None,
                "opponent_odd": None,
                "opponent_lay": None,
                "match_title": None,
                "error": str(e)
            }

    @staticmethod
    def _clean_team_name(name: str) -> str:
        if not name:
            return ""
        # Strip HTML tags
        s = re.sub(r'<[^>]+>', '', name)

        # Standardize age group descriptors (e.g. Under 19 -> U19, Under 23 -> U23)
        s = re.sub(r'\bunder[\s\-]*19s?\b', 'U19', s, flags=re.IGNORECASE)
        s = re.sub(r'\bunder[\s\-]*23s?\b', 'U23', s, flags=re.IGNORECASE)
        
        # Cut off at live score patterns (e.g. 119-8, 18-1, 0-0, 20.0) or commentary markers
        s = re.split(
            r'\b\d+[\/\-]\d+\b|\b\d+\.\d+\b|\b\d+\s*\(|\b(opt to|need|runs|wickets|overs|scorecard|commentary|live score|highlight|won|lost|by|playing|toss)\b',
            s,
            flags=re.IGNORECASE
        )[0]
        
        # Strip match format descriptors & ordinals (e.g. 1st ODI, 2nd T20I, 3rd Test, Match 15, 4th-odi, 15th-)
        s = re.sub(r'\b\d+(st|nd|rd|th)?([\s\-]+(ODI|T20I?|T20|Test|Match|ODI Match))?\b', ' ', s, flags=re.IGNORECASE)
        s = re.sub(r'\b(Live Score|Match Updates|Live Cricket Score|CREX|Live Scorecard|Scorecard|Updates|Match|Tour|Series|In-Play|In Play)\b', ' ', s, flags=re.IGNORECASE)
        
        # Strip unwanted punctuation (keep letters, numbers, spaces, hyphens)
        s = re.sub(r'[^\w\s\-]', ' ', s)
        
        words = s.strip().split()
        if not words:
            return name.strip()
            
        # Expand standalone abbreviation if single word match
        if len(words) == 1 and words[0].upper() in TEAM_ABBREVIATIONS:
            return TEAM_ABBREVIATIONS[words[0].upper()]
            
        # Reconstruct full team name preserving all words and numbers (e.g. "Pakistan U19", "Botswana", "West Indies Women")
        cleaned = " ".join(
            TEAM_ABBREVIATIONS[w.upper()] if w.upper() in TEAM_ABBREVIATIONS else (w.upper() if w.upper() in ["U19", "U23", "T20", "ODI", "XI"] else w.capitalize())
            for w in words
        )
        return cleaned

    @staticmethod
    def _strip_team_suffixes(s: str) -> str:
        if not s:
            return ""
        t = s.lower().strip()
        # Remove parentheses noise (e.g. '(FAV)')
        t = re.sub(r'\(.*?\)', '', t)
        # Remove common sports suffixes: U19, WOMEN, MEN, ELIMINATOR, FAV, W, XI, SQUAD, TEAM
        t = re.sub(r'[\s\-]+(u19|under[\s\-]*19|women|men|eliminator|fav|xi|squad|team)\b', '', t, flags=re.IGNORECASE)
        # Remove trailing -w, -W, w, W, e.g. "ham-w" -> "ham", "bt-w" -> "bt", "surrey-w" -> "surrey"
        t = re.sub(r'[\s\-]w$', '', t, flags=re.IGNORECASE)
        t = re.sub(r'\bw$', '', t, flags=re.IGNORECASE)
        return t.strip()

    @staticmethod
    def _score_team_match(query: str, target: str) -> int:
        """
        Returns a priority score (> 0) for matching query against target team/card.
        Prioritizes exact card tokens, stem matches, and gender alignment.
        Disambiguates Hampshire Men vs Hampshire Women without misrouting odds.
        """
        if not query or not target:
            return 0

        q_raw = query.strip()
        t_raw = target.strip()

        # 1. Exact string match (case insensitive) -> Highest Priority
        if q_raw.lower() == t_raw.lower():
            return 1000

        # Gender indicator check
        q_women = bool(re.search(r'[\s\-]w\b|\bwomen\b|\bfemale\b', q_raw, re.IGNORECASE))
        t_women = bool(re.search(r'[\s\-]w\b|\bwomen\b|\bfemale\b', t_raw, re.IGNORECASE))
        q_men = bool(re.search(r'\bmen\b|\bmale\b', q_raw, re.IGNORECASE))
        t_men = bool(re.search(r'\bmen\b|\bmale\b', t_raw, re.IGNORECASE))

        # Disqualify cross-gender mismatch (e.g., Hampshire Men vs Hampshire Women)
        if q_women and (t_men or (not t_women and "men" in t_raw.lower())):
            return 0
        if q_men and t_women:
            return 0

        # Exact match-card token check (e.g. "Ham-w", "Bt-w", "England U19")
        if ExchangeScraperEngine._is_strict_team_match(q_raw, t_raw):
            score = 500
            if q_women == t_women:
                score += 200
            q_clean = ExchangeScraperEngine._clean_team_name(q_raw).lower()
            t_clean = ExchangeScraperEngine._clean_team_name(t_raw).lower()
            if q_clean == t_clean:
                score += 150
            return score

        return 0

    @staticmethod
    def _is_strict_team_match(query: str, target: str) -> bool:
        """
        Relaxed & Smart Team Name Matching with Abbreviation and Substring Mapping.
        Matches "SURREY" with "Surrey Women Eliminator", "Ham" with "Ham-w", "BT" with "Bt-w".
        Guards against cross-country leaks and U19/senior mismatch when explicitly specified.
        """
        if not query or not target:
            return False

        if is_team_match(query, target):
            return True

        q_raw = query.strip()
        t_raw = target.strip()

        # 1. Exact string match (case-insensitive)
        if q_raw.lower() == t_raw.lower():
            return True

        q_clean = ExchangeScraperEngine._clean_team_name(query).lower()
        t_clean = ExchangeScraperEngine._clean_team_name(target).lower()

        if q_clean == t_clean or is_team_match(q_clean, t_clean):
            return True

        # 2. Country Cross-Leak Prevention Guard
        countries = {
            "england", "pakistan", "india", "australia", "zimbabwe", "sri lanka",
            "south africa", "new zealand", "west indies", "bangladesh", "afghanistan",
            "uganda", "botswana", "barbados", "jamaica"
        }
        q_words_raw = set(re.split(r'\W+', q_clean))
        t_words_raw = set(re.split(r'\W+', t_clean))

        q_countries = {w for w in q_words_raw if w in countries}
        t_countries = {w for w in t_words_raw if w in countries}

        if q_countries and t_countries and q_countries != t_countries:
            return False

        # 3. Explicit Age Group Guard (U19 vs Senior)
        q_has_u19 = "u19" in q_raw.lower() or "under 19" in q_raw.lower() or "u-19" in q_raw.lower()
        t_has_u19 = "u19" in t_raw.lower() or "under 19" in t_raw.lower() or "u-19" in t_raw.lower()
        if q_has_u19 != t_has_u19:
            return False

        # 4. Strip suffixes to compare base team stems
        q_stem = ExchangeScraperEngine._strip_team_suffixes(q_clean)
        t_stem = ExchangeScraperEngine._strip_team_suffixes(t_clean)

        if q_stem and t_stem:
            if q_stem == t_stem:
                return True

            # Substring / startswith / contains check on base stems
            if q_stem in t_stem or t_stem in q_stem:
                return True
            if q_stem.startswith(t_stem) or t_stem.startswith(q_stem):
                return True

        # 5. Raw Substring / startswith / contains check
        q_lower = q_raw.lower()
        t_lower = t_raw.lower()
        if q_lower in t_lower or t_lower in q_lower:
            return True
        if q_clean in t_clean or t_clean in q_clean:
            return True

        # 6. Abbreviation & Alias Mapping
        if ExchangeScraperEngine._check_alias_match(q_raw, t_raw) or ExchangeScraperEngine._check_alias_match(q_clean, t_clean):
            return True

        # 7. Word set intersection check (for multi-word teams)
        q_words = [w for w in re.split(r'\W+', q_stem or q_clean) if len(w) >= 2]
        t_words = [w for w in re.split(r'\W+', t_stem or t_clean) if len(w) >= 2]

        if q_words and t_words:
            matching = set(q_words).intersection(set(t_words))
            if len(matching) >= min(len(q_words), len(t_words)) and len(matching) > 0:
                return True

        return False

    @staticmethod
    def _check_alias_match(query: str, target: str) -> bool:
        q = query.upper()
        t = target.upper()

        q_stem = ExchangeScraperEngine._strip_team_suffixes(q)
        t_stem = ExchangeScraperEngine._strip_team_suffixes(t)

        for k, v in TEAM_ABBREVIATIONS.items():
            k_u = k.upper()
            v_u = v.upper()

            # If query equals or starts with abbreviation
            if q == k_u or q_stem == k_u or q.startswith(k_u):
                if v_u in t or v_u in t_stem or k_u in t or k_u in t_stem or t.startswith(k_u.lower()):
                    return True
            # If target equals or starts with abbreviation
            if t == k_u or t_stem == k_u or t.startswith(k_u):
                if v_u in q or v_u in q_stem or k_u in q or k_u in q_stem or q.startswith(k_u.lower()):
                    return True
            # General abbreviation / name cross match
            if (k_u in q or v_u in q) and (k_u in t or v_u in t):
                return True
        return False

global_exchange_scraper = ExchangeScraperEngine()
