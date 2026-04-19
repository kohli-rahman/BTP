from datetime import datetime, timezone

import requests

from wp1_data_collection.collectors.base_collector import BaseCollector
from wp1_data_collection.config.settings import OPENWEATHER_API_KEY, MONITORED_LOCATIONS
from wp1_data_collection.storage.schema import WeatherRecord
from wp1_data_collection.utils.logger import logger
from wp1_data_collection.utils.rate_limiter import retry_on_rate_limit

OWM_CURRENT_URL  = "https://api.openweathermap.org/data/2.5/weather"
OWM_FORECAST_URL = "https://api.openweathermap.org/data/2.5/forecast"
OWM_ONECALL_URL  = "https://api.openweathermap.org/data/3.0/onecall"


class WeatherCollector(BaseCollector):
    """
    Fetches current weather and rainfall data for monitored hilly regions
    using OpenWeatherMap API.

    Each location in MONITORED_LOCATIONS gets its own WeatherRecord snapshot.
    High rainfall thresholds trigger WARNING annotations in the record.
    """

    # Rainfall thresholds (mm/hr) for hazard annotation
    RAINFALL_WARNING_THRESHOLD  = 15.0
    RAINFALL_CRITICAL_THRESHOLD = 35.0

    def __init__(self, locations: list = None, **kwargs):
        super().__init__("WeatherCollector", **kwargs)
        self.locations = locations or MONITORED_LOCATIONS

    def authenticate(self) -> bool:
        if not OPENWEATHER_API_KEY:
            logger.warning("[WeatherCollector] OPENWEATHER_API_KEY not set — skipping.")
            return False
        resp = requests.get(
            OWM_CURRENT_URL,
            params={"lat": 28.6, "lon": 77.2, "appid": OPENWEATHER_API_KEY},
            timeout=10,
        )
        if resp.status_code == 200:
            logger.info("[WeatherCollector] API key validated.")
            return True
        logger.error(f"[WeatherCollector] Auth probe failed: {resp.status_code}")
        return False

    def _fetch_current(self, lat: float, lon: float) -> dict:
        resp = requests.get(
            OWM_CURRENT_URL,
            params={"lat": lat, "lon": lon, "appid": OPENWEATHER_API_KEY, "units": "metric"},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()

    def _parse_weather(self, raw: dict, location: dict) -> WeatherRecord:
        main    = raw.get("main", {})
        wind    = raw.get("wind", {})
        rain    = raw.get("rain", {})
        weather = raw.get("weather", [{}])[0]

        rainfall_1h = rain.get("1h", 0.0)

        condition = weather.get("description", "")
        if rainfall_1h >= self.RAINFALL_CRITICAL_THRESHOLD:
            condition = f"[CRITICAL RAINFALL] {condition}"
        elif rainfall_1h >= self.RAINFALL_WARNING_THRESHOLD:
            condition = f"[WARNING RAINFALL] {condition}"

        return WeatherRecord(
            location_name=location["name"],
            lat=location["lat"],
            lon=location["lon"],
            collected_at=self.utc_now(),
            temperature_c=main.get("temp"),
            humidity_pct=main.get("humidity"),
            wind_speed_ms=wind.get("speed"),
            wind_dir_deg=wind.get("deg"),
            rainfall_mm=rainfall_1h,
            pressure_hpa=main.get("pressure"),
            condition=condition,
            raw_payload=raw,
        )

    @retry_on_rate_limit(max_retries=3, base_wait=30.0)
    def collect_batch(self, **kwargs) -> list[WeatherRecord]:
        records = []
        for loc in self.locations:
            try:
                raw = self._fetch_current(loc["lat"], loc["lon"])
                record = self._parse_weather(raw, loc)
                records.append(record)
                logger.debug(f"[WeatherCollector] {loc['name']}: {record.condition}, rain={record.rainfall_mm}mm")
            except Exception as e:
                logger.error(f"[WeatherCollector] Failed for {loc['name']}: {e}")

        logger.info(f"[WeatherCollector] Collected weather for {len(records)} locations.")
        return records

    def stream(self, interval_seconds: int = 1800, **kwargs):
        """Poll weather every interval_seconds (default 30 min)."""
        import time
        logger.info(f"[WeatherCollector] Polling every {interval_seconds}s.")
        while True:
            try:
                self.run_batch()
            except Exception as e:
                logger.error(f"[WeatherCollector] Poll error: {e}")
            time.sleep(interval_seconds)
