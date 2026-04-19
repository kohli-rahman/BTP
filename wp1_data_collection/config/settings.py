import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
LOG_DIR = DATA_DIR / "logs"

# Social Media
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "")
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY", "")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET", "")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN", "")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET", "")

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "DisasterCollector/1.0")

# News
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

# Weather
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")

# Disaster keyword sets used across all collectors
DISASTER_KEYWORDS = [
    "landslide", "flood", "earthquake", "cloudburst", "cyclone",
    "tsunami", "avalanche", "flash flood", "disaster", "natural hazard",
    "Uttarakhand disaster", "Himachal disaster", "Sikkim flood",
    "Kerala flood", "rescue operations", "NDRF", "SDRF",
]

# Monitored locations (lat, lon, radius_km) for weather/spatial queries
MONITORED_LOCATIONS = [
    {"name": "Uttarakhand", "lat": 30.0668, "lon": 79.0193, "radius_km": 200},
    {"name": "Sikkim",      "lat": 27.5330, "lon": 88.5122, "radius_km": 100},
    {"name": "Kerala",      "lat": 10.8505, "lon": 76.2711, "radius_km": 200},
    {"name": "Himachal",    "lat": 31.1048, "lon": 77.1734, "radius_km": 200},
]

# Subreddits relevant to disaster events
DISASTER_SUBREDDITS = [
    "india", "IndiaSpeaks", "worldnews", "naturaldisasters",
    "Uttarakhand", "Kerala", "earthquake",
]

# RSS news feeds for disaster/weather news
RSS_FEEDS = {
    "ndtv_india":        "https://feeds.feedburner.com/ndtvnews-india-news",
    "thehindu_national": "https://www.thehindu.com/news/national/feeder/default.rss",
    "reliefweb":         "https://reliefweb.int/disasters/rss.xml",
    "gdacs":             "https://www.gdacs.org/xml/rss.xml",
    "ndma_india":        "https://ndma.gov.in/rss.xml",
    "downtoearth":       "https://www.downtoearth.org/rss/natural-disasters",
}

# Streaming config
STREAM_RECONNECT_WAIT = 5   # seconds before reconnect on stream error
RATE_LIMIT_PAUSE     = 60   # seconds to pause when rate-limited

# Batch collection window (days to look back for historical data)
BATCH_LOOKBACK_DAYS = 7
