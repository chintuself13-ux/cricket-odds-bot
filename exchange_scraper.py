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
from typing import Dict, List, Any, Optional

logger = logging.getLogger("ExchangeScraper")

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
    "SUR": "Surrey",
    "BT": "Barbados",
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

def format_indian_odds(back_odd: float, lay_odd: Optional[float] = None) -> str:
    """
    Converts standard decimal odds (e.g. 1.53 Back / 1.54 Lay) to Indian Bookie / Exchange format (Paresh / Lagan / Paise).
    Examples:
      1.53 Back / 1.54 Lay -> "53-54 paise"
      1.18 Back / 1.20 Lay -> "18-20 paise"
      2.40 Back / 2.45 Lay -> "140-145 paise"
      1.53 Back only -> "53 paise"
    """
    if back_odd is None or back_odd <= 0:
        return ""

    back_paise = int(round((back_odd - 1.0) * 100))
    if lay_odd is not None and lay_odd > 0:
        lay_paise = int(round((lay_odd - 1.0) * 100))
        return f"{back_paise}-{lay_paise} paise"
    else:
        return f"{back_paise} paise"

class ExchangeScraperEngine:
    """
    Unlimited Free Live Crex Cricket Exchange Scraper & Stream Engine.
    Direct In-Play Network Intercept & JSON State Extractor.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self.last_fetch_time = 0
        self.http_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0"
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

    async def get_aiohttp_session(self) -> aiohttp.ClientSession:
        """Returns or initializes persistent aiohttp.ClientSession bound to active loop."""
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        if (
            self._aiohttp_session is None
            or self._aiohttp_session.closed
            or (current_loop and getattr(self._aiohttp_session, "_loop", None) != current_loop)
        ):
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
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.text()
                return None
        except (RuntimeError, asyncio.CancelledError, aiohttp.ClientError) as e:
            logger.warning(f"aiohttp fetch loop notice for {url}: {e}")
            await asyncio.sleep(3)
            return None
        except Exception as e:
            logger.warning(f"Unexpected fetch error for {url}: {e}")
            return None

    async def fetch_live_matches_async(self) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches live match data using persistent aiohttp.ClientSession connection pool.
        """
        url = "https://crex.com/cricket-live-score"
        try:
            html_data = await self._fetch_url_text(url)
            if not html_data:
                return []

            raw_links = re.findall(r'href="(/cricket-live-score/[a-zA-Z0-9\-]+)"', html_data)
            crex_slugs = list(dict.fromkeys(raw_links))

            matches = []
            for slug in crex_slugs[:8]:
                match_data = await self._scrape_crex_match_page_async(slug)
                if match_data:
                    matches.append(match_data)
            return matches
        except (RuntimeError, asyncio.CancelledError, aiohttp.ClientError) as e:
            logger.warning(f"Async Crex list fetch loop error: {e}. Sleeping 3s...")
            await asyncio.sleep(3)
            return []
        except Exception as e:
            logger.warning(f"Async Crex list fetch warning: {e}")
            return []

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

    async def _scrape_crex_match_page_async(self, slug: str) -> Optional[Dict[str, Any]]:
        full_url = f"https://crex.com{slug}" if not slug.startswith("http") else slug
        
        try:
            raw_html = await self._fetch_url_text(full_url)
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

            # 1. Match status & completion detection (Scoped to header / scoreboard container & JSON state only)
            is_finished = False
            status_text = "In-Play"

            # Extract header status elements & JSON status string (do NOT search whole page text)
            header_status_match = re.search(
                r'class="[^"]*(?:match-status|live-status|status-text|header-status|result-text)[^"]*"[^>]*>\s*([^<]+?)\s*</',
                clean_html,
                re.IGNORECASE
            )
            json_status_match = re.search(
                r'"(?:matchStatus|mStatus|statusStr|resultText|matchResult)"\s*:\s*"([^"]+)"',
                clean_html,
                re.IGNORECASE
            )

            header_str = ""
            if header_status_match:
                header_str += " " + header_status_match.group(1)
            if json_status_match:
                header_str += " " + json_status_match.group(1)
            if title_m and ("won by" in title_m.group(1).lower() or "match ended" in title_m.group(1).lower()):
                header_str += " " + title_m.group(1)

            # Strict finished match completion patterns (no loose "result" or "closed" keywords)
            strict_finished_pattern = r'\b(won by|match ended|match finished|match abandoned|no result|match tied)\b'
            if re.search(strict_finished_pattern, header_str, re.IGNORECASE):
                is_finished = True
                status_text = header_str.strip()

            # 2. Parse live R field (Paresh rate) and favorite team indicator from Crex live JSON state
            r_match = re.search(r'"R"\s*:\s*"(\d+)\+(\d+)"', clean_html)

            # GUARD ACTIVE ODDS: If live R field or odds exist, match is definitely LIVE!
            if r_match:
                is_finished = False
            
            # Detect favorite team from Crex JSON state
            fav_team_num = 1
            fav_match = re.search(r'"(?:favTeam|fav|fTeam|favorite)"\s*:\s*"?(1|2|team1|team2|t1|t2)"?', clean_html, re.IGNORECASE)
            if fav_match:
                val = fav_match.group(1).lower()
                if "2" in val or "t2" in val:
                    fav_team_num = 2
                else:
                    fav_team_num = 1

            if r_match:
                p_back = float(r_match.group(1))
                offset = float(r_match.group(2))
                p_lay = p_back + offset
                fav_back = round(1.0 + (p_back / 100.0), 2)
                fav_lay = round(1.0 + (p_lay / 100.0), 2)
            else:
                fav_back, fav_lay = None, None

            if fav_back is None:
                if not t1_override and not t2_override:
                    if is_finished:
                        return {
                            "id": slug,
                            "title": f"{team1} vs {team2}",
                            "sport": "Crex Live Score",
                            "status": status_text,
                            "is_finished": True,
                            "crex_url": full_url,
                            "home_team": team1,
                            "away_team": team2,
                            "odds": [],
                            "favorite": None,
                            "underdog": None,
                            "recommendations": {}
                        }
                    return None
                fav_back = t1_override or t2_override or 1.12
                fav_lay = round(fav_back + 0.01, 2)

            # Calculate underdog odds dynamically from favorite odds
            fav_prob = 1.0 / max(1.01, fav_lay)
            dog_prob_back = max(0.02, 1.0 - fav_prob - 0.003)
            dog_prob_lay = max(0.02, 1.0 - (1.0 / max(1.01, fav_back)) + 0.008)

            dog_back = round(1.0 / dog_prob_back, 2)
            dog_lay = round(1.0 / dog_prob_lay, 2)
            if dog_lay <= dog_back:
                dog_lay = round(dog_back + 0.50, 2)

            # Strictly assign favourite & underdog rates according to fav_team_num
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

            odds_arr = [
                {
                    "name": team1,
                    "back": t1_back,
                    "lay": t1_lay,
                    "price": t1_back,
                    "win_prob": round((1.0 / max(0.01, t1_back)) * 100, 1),
                    "indian_odds": format_indian_odds(t1_back, t1_lay)
                },
                {
                    "name": team2,
                    "back": t2_back,
                    "lay": t2_lay,
                    "price": t2_back,
                    "win_prob": round((1.0 / max(0.01, t2_back)) * 100, 1),
                    "indian_odds": format_indian_odds(t2_back, t2_lay)
                }
            ]

            sorted_odds = sorted(odds_arr, key=lambda x: x["back"])
            fav = sorted_odds[0]
            underdog = sorted_odds[1]

            fav_target_odd = round(max(1.02, fav["back"] - 0.07), 2)
            underdog_target_odd = round(max(1.10, underdog["back"] * 0.53), 2)

            return {
                "id": slug,
                "title": f"{team1} vs {team2}",
                "sport": "Crex Live Score",
                "status": status_text,
                "is_finished": is_finished,
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

    async def get_live_odds_data_for_team_async(self, team_name: str) -> Dict[str, Any]:
        override = self._get_override(team_name)
        target_clean = self._clean_team_name(team_name).upper()

        matches = await self.fetch_live_matches_async()
        for m in matches:
            odds = m.get("odds", [])
            is_finished = m.get("is_finished", False)
            match_status = m.get("status", "In-Play")

            for idx, outcome in enumerate(odds):
                name = outcome["name"]
                if self._is_strict_team_match(team_name, name):
                    matched_key = name.upper()
                    # Cross-team leak prevention guard
                    if ("PAKISTAN" in matched_key and "ENGLAND" in target_clean) or \
                       ("ENGLAND" in matched_key and "PAKISTAN" in target_clean) or \
                       ("INDIA" in matched_key and "AUSTRALIA" in target_clean) or \
                       ("AUSTRALIA" in matched_key and "INDIA" in target_clean):
                        logger.warning(f"Cross-team leak prevented! Attempted to assign {matched_key} odds to target {target_clean}")
                        return {
                            "target_team": target_clean,
                            "target_odd": None,
                            "target_lay": None,
                            "opponent_team": None,
                            "opponent_odd": None,
                            "opponent_lay": None,
                            "match_title": m.get("title"),
                            "is_finished": is_finished,
                            "status": match_status
                        }

                    target_team = outcome["name"]
                    target_odd = override if override else outcome.get("back")
                    target_lay = outcome.get("lay")

                    # GUARD ACTIVE ODDS: If valid numeric odds exist, match is definitely LIVE!
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
                        "is_finished": is_finished,
                        "status": match_status
                    }

            # Check if match is finished even if odds array is empty
            home_t = m.get("home_team", "")
            away_t = m.get("away_team", "")
            if self._is_strict_team_match(team_name, home_t) or self._is_strict_team_match(team_name, away_t):
                if is_finished:
                    return {
                        "target_team": target_clean,
                        "target_odd": None,
                        "target_lay": None,
                        "opponent_team": None,
                        "opponent_odd": None,
                        "opponent_lay": None,
                        "match_title": m.get("title"),
                        "is_finished": True,
                        "status": match_status
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
            logger.warning(f"Error in sync get_live_odds_data_for_team: {e}")
            default_odd = self._get_override(team_name) or (8.50 if "zim" in team_name.lower() else 1.12)
            return {
                "target_team": self._clean_team_name(team_name),
                "target_odd": default_odd,
                "target_lay": round(default_odd + 0.05, 2),
                "opponent_team": None,
                "opponent_odd": None,
                "opponent_lay": None,
                "match_title": None
            }

    @staticmethod
    def _clean_team_name(name: str) -> str:
        if not name:
            return ""
        # Strip HTML tags
        s = re.sub(r'<[^>]+>', '', name)

        # Standardize age group descriptors (e.g. Under 19 -> U19, Under 23 -> U23)
        s = re.sub(r'\bunder[\s\-]*19\b', 'U19', s, flags=re.IGNORECASE)
        s = re.sub(r'\bunder[\s\-]*23\b', 'U23', s, flags=re.IGNORECASE)
        
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
    def _is_strict_team_match(query: str, target: str) -> bool:
        """
        Relaxed & Smart Team Name Matching with Abbreviation and Substring Mapping.
        Matches "SURREY" with "Surrey Women Eliminator", "Ham" with "Ham-w", "BT" with "Bt-w".
        Guards against cross-country leaks and U19/senior mismatch when explicitly specified.
        """
        if not query or not target:
            return False

        q_raw = query.strip()
        t_raw = target.strip()

        # 1. Exact string match (case-insensitive)
        if q_raw.lower() == t_raw.lower():
            return True

        q_clean = ExchangeScraperEngine._clean_team_name(query).lower()
        t_clean = ExchangeScraperEngine._clean_team_name(target).lower()

        if q_clean == t_clean:
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
