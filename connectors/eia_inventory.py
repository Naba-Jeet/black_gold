"""
connectors/eia_inventory.py
────────────────────────────
Fetches and processes crude oil inventory data from two sources:

  1. EIA (Energy Information Administration) — Official weekly report
       → Published every Wednesday ~10:30 AM ET (8:00 PM IST)
       → Endpoint: api.eia.gov/v2/petroleum
       → Free API key: https://www.eia.gov/opendata/register.php

  2. API (American Petroleum Institute) — Industry estimate
       → Published Tuesday evening (ahead of EIA by ~16 hours)
       → No official API — scraped from Reuters/OilPrice headlines
       → Acts as a leading signal for EIA surprise direction

Output schema cached in Redis:
  mcx:eia:latest → {
      actual_draw_bbl, estimate_bbl, surprise_bbl,
      four_week_avg, trend, signal, ts
  }
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
import aiohttp
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup

# NOTE: install beautifulsoup4 for API scraper: pip install beautifulsoup4
# For production, replace the scraper with a paid Refinitiv/Bloomberg feed.

from core.config import TTL, EIA_API_KEY, cache

log = logging.getLogger("EIAConnector")

# ─────────────────────────────────────────────────────────────
# EIA SERIES CODES
# ─────────────────────────────────────────────────────────────
EIA_SERIES = {
    "crude_stocks"     : "WCRSTUS1",   # US ending stocks of crude oil (1000 bbl)
    "cushing_stocks"   : "WCSCUS2",    # Cushing, OK crude stocks
    "spr_stocks"       : "WCSSTUS2",   # Strategic Petroleum Reserve
    "refinery_inputs"  : "WCRRIUS2",   # Crude inputs to refineries
    "crude_imports"    : "WCRIMUS2",   # US crude imports
    "crude_production" : "WCRFPUS2",   # US crude field production
}

EIA_BASE_URL = "https://api.eia.gov/v2/petroleum/sum/sndw/data/"

# ─────────────────────────────────────────────────────────────
# API ESTIMATE SCRAPE TARGETS
# ─────────────────────────────────────────────────────────────
# In production replace with a paid newswire feed (Reuters, Bloomberg)
API_ESTIMATE_URLS = [
    "https://oilprice.com/Energy/Energy-General/crude-oil/",
    "https://www.reuters.com/business/energy/",
]


# ─────────────────────────────────────────────────────────────
# EIA CONNECTOR
# ─────────────────────────────────────────────────────────────
class EIAInventoryConnector:
    """
    Fetches EIA weekly petroleum inventory data.

    Usage:
        eia = EIAInventoryConnector()
        data = await eia.fetch_all()
    """

    def __init__(self):
        if not EIA_API_KEY:
            log.warning("EIA_API_KEY not set — requests will fail for paid series.")

    # ── main entry point ───────────────────────
    async def fetch_all(self) -> dict:
        """
        Fetch all configured EIA series concurrently.
        Returns a combined dict with processed signals.
        """
        log.info("Fetching EIA inventory data...")
        async with aiohttp.ClientSession() as session:
            tasks = {
                name: self._fetch_series(session, series_id, weeks=8)
                for name, series_id in EIA_SERIES.items()
            }
            results = {}
            for name, coro in tasks.items():
                try:
                    results[name] = await coro
                except Exception as e:
                    log.error("Failed to fetch EIA series [%s]: %s", name, e)
                    results[name] = []

        processed = self._process(results)
        self._cache(processed)
        return processed

    # ── fetch a single EIA series ──────────────
    async def _fetch_series(
        self, session: aiohttp.ClientSession, series_id: str, weeks: int = 8
    ) -> list[dict]:
        params = {
            "api_key"         : EIA_API_KEY,
            "frequency"       : "weekly",
            "data[0]"         : "value",
            "facets[series][]": series_id,
            "sort[0][column]" : "period",
            "sort[0][direction]": "desc",
            "length"          : weeks,
        }
        async with session.get(EIA_BASE_URL, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                raise ValueError(f"EIA API returned HTTP {resp.status}")
            body = await resp.json()
            return body.get("response", {}).get("data", [])

    # ── compute signals from raw data ──────────
    def _process(self, results: dict) -> dict:
        crude = results.get("crude_stocks", [])
        if len(crude) < 2:
            return {"error": "Insufficient EIA data", "ts": datetime.utcnow().isoformat()}

        # Latest and prior week values (in 1000 bbl → convert to bbl)
        latest_val = float(crude[0].get("value", 0)) * 1000
        prior_val  = float(crude[1].get("value", 0)) * 1000
        latest_dt  = crude[0].get("period", "")

        weekly_change = latest_val - prior_val  # negative = draw, positive = build

        # 4-week average change
        changes = []
        for i in range(min(4, len(crude) - 1)):
            changes.append(
                (float(crude[i].get("value", 0)) - float(crude[i + 1].get("value", 0))) * 1000
            )
        four_week_avg = sum(changes) / len(changes) if changes else 0

        # Direction signal
        if weekly_change < -2_000_000:
            signal = "STRONG_DRAW_BULLISH"
        elif weekly_change < 0:
            signal = "DRAW_BULLISH"
        elif weekly_change < 2_000_000:
            signal = "SMALL_BUILD_BEARISH"
        else:
            signal = "LARGE_BUILD_BEARISH"

        # Cushing stocks (delivery point for WTI futures)
        cushing = results.get("cushing_stocks", [])
        cushing_latest  = float(cushing[0].get("value", 0)) * 1000 if cushing else 0
        cushing_prior   = float(cushing[1].get("value", 0)) * 1000 if len(cushing) > 1 else 0
        cushing_change  = cushing_latest - cushing_prior

        # Refinery utilisation proxy
        refinery = results.get("refinery_inputs", [])
        refinery_latest = float(refinery[0].get("value", 0)) * 1000 if refinery else 0

        return {
            "report_date"    : latest_dt,
            "crude_stocks_bbl"     : int(latest_val),
            "weekly_change_bbl"    : int(weekly_change),
            "four_week_avg_bbl"    : int(four_week_avg),
            "cushing_stocks_bbl"   : int(cushing_latest),
            "cushing_change_bbl"   : int(cushing_change),
            "refinery_inputs_bbl"  : int(refinery_latest),
            "spr_stocks_bbl"       : int(float((results.get("spr_stocks") or [{}])[0].get("value", 0)) * 1000),
            "signal"               : signal,
            "is_draw"              : weekly_change < 0,
            "draw_bbl"             : abs(int(weekly_change)) if weekly_change < 0 else 0,
            "build_bbl"            : int(weekly_change) if weekly_change > 0 else 0,
            "ts"                   : datetime.utcnow().isoformat(),
            "source"               : "EIA",
        }

    # ── write to Redis ─────────────────────────
    def _cache(self, data: dict):
        cache.set("mcx:eia:latest", data, TTL["eia"])
        cache.publish("channel:eia", data)
        log.info(
            "EIA cached — Change: %+.2fM bbl | Signal: %s",
            data.get("weekly_change_bbl", 0) / 1_000_000,
            data.get("signal"),
        )


# ─────────────────────────────────────────────────────────────
# API ESTIMATE SCRAPER (runs Tuesday evening before EIA Wed)
# ─────────────────────────────────────────────────────────────
class APIEstimateScraper:
    """
    Scrapes the American Petroleum Institute (API) inventory estimate
    published every Tuesday. This is a leading indicator — if API shows
    a large draw, expect EIA to confirm bullish, and vice versa.

    PRODUCTION NOTE:
        Replace the web scraper below with a licensed newswire feed:
        - Refinitiv Eikon API  (paid)
        - Bloomberg BAPI       (paid)
        - Platts news API      (paid)
        The scraper is provided for prototyping only.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
            )
        })

    def fetch(self) -> dict:
        """
        Returns the latest API estimate as a dict.
        Falls back to a None value if scraping fails.
        """
        log.info("Fetching API (industry) estimate...")
        for url in API_ESTIMATE_URLS:
            try:
                result = self._scrape(url)
                if result.get("estimate_bbl") is not None:
                    self._cache(result)
                    return result
            except Exception as e:
                log.warning("API scrape failed [%s]: %s", url, e)

        log.error("All API estimate sources failed.")
        return {"estimate_bbl": None, "source": "API", "ts": datetime.utcnow().isoformat()}

    def _scrape(self, url: str) -> dict:
        resp = self.session.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Look for headlines containing inventory keywords
        estimate_bbl = None
        direction    = None
        for tag in soup.find_all(["h1", "h2", "h3", "p", "span"]):
            text = tag.get_text(strip=True).lower()
            if "api" in text and ("crude" in text or "inventory" in text or "barrel" in text):
                # Extract numeric values — e.g. "API: -3.5M barrels" or "draw of 2.1 million"
                import re
                matches = re.findall(r"([+-]?\d+\.?\d*)\s*(m|million|mln|k|thousand)?", text)
                for val_str, unit in matches:
                    val = float(val_str)
                    if unit in ("m", "million", "mln"):
                        val *= 1_000_000
                    elif unit in ("k", "thousand"):
                        val *= 1_000
                    if 500_000 < abs(val) < 15_000_000:
                        estimate_bbl = int(val)
                        direction = "DRAW" if val < 0 else "BUILD"
                        break
            if estimate_bbl is not None:
                break

        return {
            "estimate_bbl": estimate_bbl,
            "direction"   : direction,
            "source"      : "API_ESTIMATE",
            "scraped_from": url,
            "ts"          : datetime.utcnow().isoformat(),
        }

    def _cache(self, data: dict):
        cache.set("mcx:api_estimate:latest", data, TTL["eia"])
        cache.publish("channel:api_estimate", data)
        log.info("API estimate cached — %s bbl (%s)", data.get("estimate_bbl"), data.get("direction"))


# ─────────────────────────────────────────────────────────────
# SURPRISE CALCULATOR — run after both EIA + API are available
# ─────────────────────────────────────────────────────────────
def compute_inventory_surprise(analyst_estimate_bbl: float | None = None) -> dict:
    """
    Compare EIA actual vs analyst consensus estimate to compute
    inventory surprise. A large surprise (> 2M bbl deviation) is
    a high-impact signal.

    Args:
        analyst_estimate_bbl: Consensus estimate in bbl (negative = expected draw).
                              Pass None to use cached API estimate.
    """
    eia_data = cache.get("mcx:eia:latest")
    api_data = cache.get("mcx:api_estimate:latest")

    if not eia_data:
        return {"error": "No EIA data in cache"}

    actual_change = eia_data.get("weekly_change_bbl", 0)

    if analyst_estimate_bbl is None and api_data:
        analyst_estimate_bbl = api_data.get("estimate_bbl")

    if analyst_estimate_bbl is None:
        return {
            "actual_change_bbl" : actual_change,
            "estimate_bbl"      : None,
            "surprise_bbl"      : None,
            "surprise_signal"   : "NO_ESTIMATE",
            "impact"            : "UNKNOWN",
        }

    surprise = actual_change - analyst_estimate_bbl
    abs_surprise = abs(surprise)

    if abs_surprise > 4_000_000:
        impact = "VERY_HIGH"
    elif abs_surprise > 2_000_000:
        impact = "HIGH"
    elif abs_surprise > 1_000_000:
        impact = "MEDIUM"
    else:
        impact = "LOW"

    surprise_signal = (
        "BULLISH_SURPRISE"  if surprise < -1_000_000 else
        "BEARISH_SURPRISE"  if surprise > 1_000_000  else
        "IN_LINE"
    )

    result = {
        "actual_change_bbl" : int(actual_change),
        "estimate_bbl"      : int(analyst_estimate_bbl),
        "surprise_bbl"      : int(surprise),
        "surprise_signal"   : surprise_signal,
        "impact"            : impact,
        "ts"                : datetime.utcnow().isoformat(),
    }
    cache.set("mcx:eia:surprise", result, TTL["eia"])
    log.info("Surprise computed: %+.2fM bbl | %s | Impact: %s",
             surprise / 1_000_000, surprise_signal, impact)
    return result


# ─────────────────────────────────────────────────────────────
# STANDALONE RUNNER
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    sys.path.insert(0, "..")

    async def main():
        eia = EIAInventoryConnector()
        data = await eia.fetch_all()
        print("\n── EIA DATA ──────────────────────────")
        for k, v in data.items():
            print(f"  {k:30s}: {v}")

        api = APIEstimateScraper()
        api_data = api.fetch()
        print("\n── API ESTIMATE ──────────────────────")
        for k, v in api_data.items():
            print(f"  {k:30s}: {v}")

        surprise = compute_inventory_surprise()
        print("\n── SURPRISE CALC ─────────────────────")
        for k, v in surprise.items():
            print(f"  {k:30s}: {v}")

    asyncio.run(main())