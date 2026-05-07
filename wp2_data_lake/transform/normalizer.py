from typing import Optional

from wp2_data_lake.storage.schema import LakeRecord

# Maps hazard labels to trigger terms (checked against keywords_hit + lowercased text)
_HAZARD_TERMS = {
    "FLOOD":       ["flood", "flooding", "flash flood", "waterlog", "inundation", "deluge"],
    "LANDSLIDE":   ["landslide", "mudslide", "debris flow", "rockfall", "slope failure"],
    "EARTHQUAKE":  ["earthquake", "tremor", "seismic", "quake", "aftershock"],
    "CYCLONE":     ["cyclone", "hurricane", "typhoon", "storm surge", "tropical storm"],
    "DROUGHT":     ["drought", "dry spell", "water scarcity", "water shortage"],
    "HEATWAVE":    ["heatwave", "heat wave", "extreme heat"],
    "COLD_WAVE":   ["cold wave", "snowstorm", "blizzard", "frost", "snowfall"],
    "FIRE":        ["wildfire", "forest fire", "wildfire", "blaze"],
    "CLOUDBURST":  ["cloudburst", "cloud burst", "heavy rain", "heavy rainfall"],
}

_CRITICAL_TERMS = ["critical", "severe", "devastating", "catastrophic", "red alert", "emergency", "sos"]
_WARNING_TERMS  = ["warning", "alert", "caution", "watch", "advisory", "orange alert", "yellow alert"]


def _extract_hazard(keywords_hit: list, text: Optional[str]) -> Optional[str]:
    corpus = " ".join(keywords_hit).lower()
    if text:
        corpus += " " + text[:500].lower()
    for hazard, terms in _HAZARD_TERMS.items():
        if any(t in corpus for t in terms):
            return hazard
    return None


def _extract_severity(keywords_hit: list, text: Optional[str], condition: Optional[str]) -> str:
    if condition and ("CRITICAL" in condition or "WARNING" in condition):
        return "CRITICAL" if "CRITICAL" in condition else "WARNING"
    corpus = " ".join(keywords_hit).lower()
    if text:
        corpus += " " + text[:500].lower()
    if any(t in corpus for t in _CRITICAL_TERMS):
        return "CRITICAL"
    if any(t in corpus for t in _WARNING_TERMS):
        return "WARNING"
    return "INFO"


def _engagement(source_type: str, raw_payload: dict) -> Optional[float]:
    if source_type == "twitter":
        m = raw_payload.get("public_metrics") or {}
        if m:
            return float(
                m.get("retweet_count", 0) * 2
                + m.get("like_count", 0)
                + m.get("reply_count", 0)
                + m.get("quote_count", 0)
            )
    elif source_type == "reddit":
        score = raw_payload.get("score", 0) or 0
        comments = raw_payload.get("num_comments", 0) or 0
        return float(score + comments * 1.5)
    return None


def _normalize_raw(raw: dict) -> LakeRecord:
    source_type = (raw.get("source_type") or "").lower()
    keywords_hit = raw.get("keywords_hit") or []
    text = raw.get("text")
    location = raw.get("location") or {}
    date_partition = (raw.get("collected_at") or "")[:10]

    hazard = _extract_hazard(keywords_hit, text)
    severity = _extract_severity(keywords_hit, text, None) if hazard else None

    return LakeRecord(
        source_type=source_type,
        source_name=raw.get("source_name") or "",
        record_id=raw.get("record_id") or "",
        collected_at=raw.get("collected_at") or "",
        published_at=raw.get("published_at"),
        date_partition=date_partition,
        url=raw.get("url"),
        text=text,
        language=raw.get("language") or "en",
        keywords_hit=keywords_hit,
        hazard_type=hazard,
        hazard_severity=severity,
        location_name=location.get("name"),
        lat=location.get("lat"),
        lon=location.get("lon"),
        media_urls=raw.get("media_urls") or [],
        engagement_score=_engagement(source_type, raw.get("raw_payload") or {}),
    )


def _normalize_weather(raw: dict) -> LakeRecord:
    rainfall = raw.get("rainfall_mm") or 0.0
    condition = raw.get("condition") or ""
    date_partition = (raw.get("collected_at") or "")[:10]

    if rainfall >= 35 or "CRITICAL" in condition:
        keywords_hit = ["critical rainfall"]
        severity = "CRITICAL"
    elif rainfall >= 15 or "WARNING" in condition:
        keywords_hit = ["heavy rainfall"]
        severity = "WARNING"
    else:
        keywords_hit = []
        severity = "INFO"

    hazard = _extract_hazard(keywords_hit, condition) or ("CLOUDBURST" if rainfall >= 15 else None)

    location_name = raw.get("location_name") or ""
    collected_at = raw.get("collected_at") or ""

    return LakeRecord(
        source_type="weather",
        source_name="openweathermap",
        record_id=f"weather_{location_name}_{collected_at}",
        collected_at=collected_at,
        date_partition=date_partition,
        keywords_hit=keywords_hit,
        hazard_type=hazard,
        hazard_severity=severity,
        location_name=location_name,
        lat=raw.get("lat"),
        lon=raw.get("lon"),
        temperature_c=raw.get("temperature_c"),
        humidity_pct=raw.get("humidity_pct"),
        wind_speed_ms=raw.get("wind_speed_ms"),
        rainfall_mm=raw.get("rainfall_mm"),
        pressure_hpa=raw.get("pressure_hpa"),
        weather_condition=condition,
    )


def _normalize_media(raw: dict) -> LakeRecord:
    date_partition = (raw.get("collected_at") or "")[:10]
    media_type_val = raw.get("media_type") or ""
    if hasattr(media_type_val, "value"):
        media_type_val = media_type_val.value

    source_url = raw.get("source_url") or ""
    return LakeRecord(
        source_type="media",
        source_name="media_collector",
        record_id=raw.get("source_record_id") or source_url,
        collected_at=raw.get("collected_at") or "",
        date_partition=date_partition,
        url=source_url,
        media_urls=[source_url] if source_url else [],
        media_type=str(media_type_val),
        local_path=raw.get("local_path"),
        file_size_kb=raw.get("file_size_kb"),
    )


def normalize(raw: dict) -> Optional[LakeRecord]:
    """Detect record type and return a normalized LakeRecord, or None if unrecognisable."""
    if "temperature_c" in raw and "location_name" in raw:
        return _normalize_weather(raw)
    if "source_url" in raw and "local_path" in raw:
        return _normalize_media(raw)
    if "source_type" in raw:
        return _normalize_raw(raw)
    return None
