from __future__ import annotations
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional
from enum import Enum


class SourceType(str, Enum):
    TWITTER   = "twitter"
    REDDIT    = "reddit"
    NEWS_API  = "news_api"
    RSS       = "rss"
    WEATHER   = "weather"
    MEDIA     = "media"


class MediaType(str, Enum):
    IMAGE = "image"
    VIDEO = "video"


@dataclass
class GeoLocation:
    name: str
    lat: Optional[float] = None
    lon: Optional[float] = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class RawRecord:
    """Universal envelope for every collected item across all source types."""
    source_type:   SourceType
    source_name:   str                        # e.g. "twitter", "ndtv_rss"
    record_id:     str                        # unique ID within source
    collected_at:  str                        # ISO-8601 UTC
    published_at:  Optional[str]              # ISO-8601 UTC from source
    url:           Optional[str]
    text:          Optional[str]
    language:      str = "en"
    keywords_hit:  list[str] = field(default_factory=list)
    location:      Optional[dict] = None      # GeoLocation.to_dict()
    media_urls:    list[str] = field(default_factory=list)
    raw_payload:   dict = field(default_factory=dict)  # full original object

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class WeatherRecord:
    """Dedicated schema for weather/sensor data points."""
    location_name:  str
    lat:            float
    lon:            float
    collected_at:   str
    temperature_c:  Optional[float]
    humidity_pct:   Optional[float]
    wind_speed_ms:  Optional[float]
    wind_dir_deg:   Optional[float]
    rainfall_mm:    Optional[float]
    pressure_hpa:   Optional[float]
    condition:      Optional[str]
    raw_payload:    dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class MediaRecord:
    """Schema for downloaded image/video assets."""
    source_url:    str
    local_path:    str
    media_type:    MediaType
    collected_at:  str
    source_record_id: Optional[str] = None   # links back to RawRecord.record_id
    file_size_kb:  Optional[float] = None
    width:         Optional[int] = None
    height:        Optional[int] = None

    def to_dict(self) -> dict:
        return asdict(self)
