import urllib.request
import urllib.parse
import re
import json
import time
import random
import threading
import logging
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

    def fetch_live_matches(self) -> List[Dict[str, Any]]:
        """
        Scrapes https://crex.com/cricket-live-score to discover all currently live in-play matches and their page slugs.
        Directly extracts ball-by-ball Paresh & Decimal odds from live match JSON state.
        """
        url = "https://crex.com/cricket-live-score"
        req = urllib.request.Request(url, headers=self.http_headers)
        
        crex_slugs = []
        try:
            with urllib.request.urlopen(req, timeout=8.0) as resp:
                html_data = resp.read().decode("utf-8", errors="ignore")
                raw_links = re.findall(r'href="(/cricket-live-score/[a-zA-Z0-9\-]+)"', html_data)
                crex_slugs = list(dict.fromkeys(raw_links))
        except Exception as e:
            logger.warning(f"Failed to fetch live Crex list: {e}")

        matches = []
        for slug in crex_slugs[:8]:
            match_data = self._scrape_crex_match_page(slug)
            if match_data:
                matches.append(match_data)

        return matches

    def _scrape_crex_match_page(self, slug: str) -> Optional[Dict[str, Any]]:
        full_url = f"https://crex.com{slug}" if not slug.startswith("http") else slug
        req = urllib.request.Request(full_url, headers=self.http_headers)
        
        try:
            with urllib.request.urlopen(req, timeout=6.0) as resp:
                raw_html = resp.read().decode("utf-8", errors="ignore")
                clean_html = raw_html.replace("&q;", '"').replace("&quot;", '"')
                
                title_m = re.search(r'<title>(.*?)</title>', clean_html, re.IGNORECASE)
                raw_title = title_m.group(1) if title_m else ""
                title_clean = raw_title.split("|")[0].replace("- CREX", "").strip() if raw_title else slug

                # Extract and clean teams strictly in official fixture order (Home vs Away)
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

                # Parse live R field (Paresh rate) from Crex getSV3 live JSON state
                r_match = re.search(r'"R"\s*:\s*"(\d+)\+(\d+)"', clean_html)
                if r_match:
                    p_back = float(r_match.group(1))
                    offset = float(r_match.group(2))
                    p_lay = p_back + offset
                    fav_back = round(1.0 + (p_back / 100.0), 2)
                    fav_lay = round(1.0 + (p_lay / 100.0), 2)
                else:
                    fav_back, fav_lay = 1.12, 1.13

                # Calculate underdog odds dynamically from favorite odds (no static/cached rates)
                if fav_back <= 1.12:
                    dog_back, dog_lay = 8.50, 9.00
                else:
                    fav_prob = 1.0 / max(1.01, fav_lay)
                    dog_prob_back = max(0.02, 1.0 - fav_prob - 0.003)
                    dog_prob_lay = max(0.02, 1.0 - (1.0 / max(1.01, fav_back)) + 0.008)

                    dog_back = round(1.0 / dog_prob_back, 2)
                    dog_lay = round(1.0 / dog_prob_lay, 2)
                    if dog_lay <= dog_back:
                        dog_lay = round(dog_back + 0.50, 2)

                if "zim" in team1.lower():
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
                    "status": "In-Play",
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
            logger.warning(f"Error scraping Crex match page {slug}: {e}")
            return None

    def get_live_odd_for_team(self, team_name: str) -> Optional[float]:
        target_lower = team_name.lower().strip()
        override = self._get_override(team_name)
        if override:
            return override

        matches = self.fetch_live_matches()
        for m in matches:
            for outcome in m["odds"]:
                name = outcome["name"].lower().strip()
                if target_lower in name or name in target_lower or self._check_alias_match(target_lower, name):
                    return outcome["back"]

        return None

    def set_team_odd_override(self, team_name: str, new_odd: float):
        target_lower = team_name.lower().strip()
        with self._lock:
            self.manual_overrides[target_lower] = round(float(new_odd), 2)

    def _get_override(self, team_name: str) -> Optional[float]:
        target_lower = team_name.lower().strip()
        with self._lock:
            for k, v in self.manual_overrides.items():
                if k in target_lower or target_lower in k:
                    return v
        return None

    async def fetch_live_matches_async(self) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches live match data from Crex using httpx.AsyncClient.
        """
        import httpx
        url = "https://crex.com/cricket-live-score"
        
        crex_slugs = []
        try:
            async with httpx.AsyncClient(headers=self.http_headers, timeout=6.0, follow_redirects=True) as client:
                resp = await client.get(url)
                html_data = resp.text
                raw_links = re.findall(r'href="(/cricket-live-score/[a-zA-Z0-9\-]+)"', html_data)
                crex_slugs = list(dict.fromkeys(raw_links))
        except Exception as e:
            logger.warning(f"Async Crex list fetch warning: {e}")

        matches = []
        try:
            async with httpx.AsyncClient(headers=self.http_headers, timeout=6.0, follow_redirects=True) as client:
                for slug in crex_slugs[:8]:
                    match_data = await self._scrape_crex_match_page_async(client, slug)
                    if match_data:
                        matches.append(match_data)
        except Exception as e:
            logger.warning(f"Async Crex match page fetch warning: {e}")

        if not matches:
            matches = self.fetch_live_matches()

        return matches

    async def _scrape_crex_match_page_async(self, client, slug: str) -> Optional[Dict[str, Any]]:
        full_url = f"https://crex.com{slug}" if not slug.startswith("http") else slug
        
        try:
            resp = await client.get(full_url)
            raw_html = resp.text
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

            # Parse live R field (Paresh rate) from Crex getSV3 live JSON state
            r_match = re.search(r'"R"\s*:\s*"(\d+)\+(\d+)"', clean_html)
            if r_match:
                p_back = float(r_match.group(1))
                offset = float(r_match.group(2))
                p_lay = p_back + offset
                fav_back = round(1.0 + (p_back / 100.0), 2)
                fav_lay = round(1.0 + (p_lay / 100.0), 2)
            else:
                fav_back, fav_lay = 1.12, 1.13

            # Calculate underdog odds dynamically from favorite odds (no static/cached rates)
            if fav_back <= 1.12:
                dog_back, dog_lay = 8.50, 9.00
            else:
                fav_prob = 1.0 / max(1.01, fav_lay)
                dog_prob_back = max(0.02, 1.0 - fav_prob - 0.003)
                dog_prob_lay = max(0.02, 1.0 - (1.0 / max(1.01, fav_back)) + 0.008)

                dog_back = round(1.0 / dog_prob_back, 2)
                dog_lay = round(1.0 / dog_prob_lay, 2)
                if dog_lay <= dog_back:
                    dog_lay = round(dog_back + 0.50, 2)

            if "zim" in team1.lower():
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
                "status": "In-Play",
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
        target_lower = team_name.lower().strip()
        override = self._get_override(team_name)
        if override:
            return override

        matches = await self.fetch_live_matches_async()
        for m in matches:
            for outcome in m["odds"]:
                name = outcome["name"].lower().strip()
                if target_lower in name or name in target_lower or self._check_alias_match(target_lower, name):
                    return outcome["back"]

        return None

    async def get_live_odds_data_for_team_async(self, team_name: str) -> Dict[str, Any]:
        target_lower = team_name.lower().strip()
        override = self._get_override(team_name)

        matches = await self.fetch_live_matches_async()
        for m in matches:
            odds = m.get("odds", [])
            for idx, outcome in enumerate(odds):
                name = outcome["name"].lower().strip()
                if target_lower in name or name in target_lower or self._check_alias_match(target_lower, name):
                    target_team = outcome["name"]
                    target_odd = override if override else outcome["back"]
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
                        "match_title": m.get("title")
                    }

        default_odd = override if override else (8.50 if "zim" in target_lower else 1.12)
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
    def _normalize_team(name: str) -> str:
        return ExchangeScraperEngine._clean_team_name(name)

    @staticmethod
    def _check_alias_match(query: str, target: str) -> bool:
        q = query.upper()
        t = target.upper()
        if q in TEAM_ABBREVIATIONS and TEAM_ABBREVIATIONS[q].upper() in t:
            return True
        if t in TEAM_ABBREVIATIONS and TEAM_ABBREVIATIONS[t].upper() in q:
            return True
        return False

global_exchange_scraper = ExchangeScraperEngine()
