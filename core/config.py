"""
core/config.py
──────────────
Central configuration + Redis cache wrapper for all Layer 1 connectors.
"""

import os
import json
import logging
import redis
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ─────────────────────────────────────────────
# ENV / SECRETS
# ─────────────────────────────────────────────
EIA_API_KEY          = os.getenv("EIA_API_KEY", "hFzWsS7HwBEZarkTmd1XbOfDZxaVfUGsBijkyT2b")
ALPHA_VANTAGE_KEY    = os.getenv("ALPHA_VANTAGE_KEY", "")   # FX fallback
TWELVEDATA_KEY       = os.getenv("TWELVEDATA_KEY", "")       # Brent/WTI/FX
TRADINGVIEW_USERNAME = os.getenv("TV_USERNAME", "")
TRADINGVIEW_PASSWORD = os.getenv("TV_PASSWORD", "")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB   = int(os.getenv("REDIS_DB", 0))

# ─────────────────────────────────────────────
# SYMBOL MAP — canonical keys used across layers
# ─────────────────────────────────────────────
SYMBOLS = {
    "MCX_CRUDE" : "MCX:CRUDEOIL1!",
    "BRENT"     : "TVC:UKOIL",
    "WTI"       : "NYMEX:CL1!",
    "USDINR"    : "FX_IDC:USDINR",
}

TIMEFRAMES = ["5", "15", "60", "240", "1D"]  # TradingView resolution strings

# ─────────────────────────────────────────────
# TTL (seconds) for Redis cache keys
# ─────────────────────────────────────────────
TTL = {
    "ohlcv"     : 30,      # live bars — refresh every 30s
    "indicator" : 60,
    "eia"       : 604800,  # 7 days — weekly release
    "spread"    : 15,      # Brent-WTI spread — near real-time
    "fx"        : 10,      # USDINR — very fast moving
    "satellite" : 86400,   # daily satellite snapshot
}


# ─────────────────────────────────────────────
# REDIS CACHE MANAGER
# ─────────────────────────────────────────────
class CacheManager:
    """
    Thin Redis wrapper used by all connectors.
    Keys follow the pattern:  mcx:<source>:<symbol>:<timeframe>
    Values are always JSON strings.
    """

    def __init__(self):
        self._r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True,
        )
        self.log = logging.getLogger("CacheManager")

    # ── write ──────────────────────────────────
    def set(self, key: str, data: dict, ttl: int) -> None:
        try:
            payload = json.dumps({**data, "_ts": datetime.utcnow().isoformat()})
            self._r.setex(key, ttl, payload)
        except Exception as e:
            self.log.error("Cache write failed [%s]: %s", key, e)

    # ── read ───────────────────────────────────
    def get(self, key: str) -> dict | None:
        try:
            raw = self._r.get(key)
            return json.loads(raw) if raw else None
        except Exception as e:
            self.log.error("Cache read failed [%s]: %s", key, e)
            return None

    # ── publish to pub/sub channel ─────────────
    def publish(self, channel: str, data: dict) -> None:
        try:
            self._r.publish(channel, json.dumps(data))
        except Exception as e:
            self.log.error("Publish failed [%s]: %s", channel, e)

    # ── list all keys for a prefix ─────────────
    def keys(self, pattern: str) -> list[str]:
        return self._r.keys(pattern)

    def ping(self) -> bool:
        try:
            return self._r.ping()
        except Exception:
            return False


# singleton — imported by all connectors
cache = CacheManager()