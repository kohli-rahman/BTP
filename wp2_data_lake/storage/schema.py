from dataclasses import dataclass, field
from typing import List, Optional
import uuid


@dataclass
class LakeRecord:
    lake_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Provenance
    source_type: str = ""
    source_name: str = ""
    record_id: str = ""

    # Timestamps
    collected_at: str = ""
    published_at: Optional[str] = None
    date_partition: str = ""

    # Content
    url: Optional[str] = None
    text: Optional[str] = None
    language: str = "en"

    # Disaster signals
    keywords_hit: List[str] = field(default_factory=list)
    hazard_type: Optional[str] = None      # FLOOD, LANDSLIDE, EARTHQUAKE, …
    hazard_severity: Optional[str] = None  # CRITICAL, WARNING, INFO

    # Geospatial
    location_name: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None

    # Media
    media_urls: List[str] = field(default_factory=list)

    # Social engagement
    engagement_score: Optional[float] = None

    # Weather-specific (populated only for source_type == "weather")
    temperature_c: Optional[float] = None
    humidity_pct: Optional[float] = None
    wind_speed_ms: Optional[float] = None
    rainfall_mm: Optional[float] = None
    pressure_hpa: Optional[float] = None
    weather_condition: Optional[str] = None

    # Media asset-specific (populated only for source_type == "media")
    media_type: Optional[str] = None
    local_path: Optional[str] = None
    file_size_kb: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            "lake_id": self.lake_id,
            "source_type": self.source_type,
            "source_name": self.source_name,
            "record_id": self.record_id,
            "collected_at": self.collected_at,
            "published_at": self.published_at,
            "date_partition": self.date_partition,
            "url": self.url,
            "text": self.text,
            "language": self.language,
            "keywords_hit": self.keywords_hit,
            "hazard_type": self.hazard_type,
            "hazard_severity": self.hazard_severity,
            "location_name": self.location_name,
            "lat": self.lat,
            "lon": self.lon,
            "media_urls": self.media_urls,
            "engagement_score": self.engagement_score,
            "temperature_c": self.temperature_c,
            "humidity_pct": self.humidity_pct,
            "wind_speed_ms": self.wind_speed_ms,
            "rainfall_mm": self.rainfall_mm,
            "pressure_hpa": self.pressure_hpa,
            "weather_condition": self.weather_condition,
            "media_type": self.media_type,
            "local_path": self.local_path,
            "file_size_kb": self.file_size_kb,
        }
