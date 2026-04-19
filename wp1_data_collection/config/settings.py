import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR  = DATA_DIR / "raw"
LOG_DIR  = DATA_DIR / "logs"

# Social Media
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "")
TWITTER_API_KEY      = os.getenv("TWITTER_API_KEY", "")
TWITTER_API_SECRET   = os.getenv("TWITTER_API_SECRET", "")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN", "")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET", "")

REDDIT_CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT    = os.getenv("REDDIT_USER_AGENT", "DisasterCollector/1.0")

# News
NEWS_API_KEY         = os.getenv("NEWS_API_KEY", "")

# Weather
OPENWEATHER_API_KEY  = os.getenv("OPENWEATHER_API_KEY", "")

# ── India-specific disaster keywords ────────────────────────────────────────
# Generic hazard terms are paired with Indian location context so news and
# social media queries stay anchored to India.
DISASTER_KEYWORDS = [
    # Hazard + India scope
    "landslide India", "flood India", "earthquake India",
    "cloudburst India", "cyclone India", "flash flood India",
    "avalanche India", "tsunami India",
    # State/region-specific
    "Uttarakhand flood", "Uttarakhand landslide", "Uttarakhand cloudburst",
    "Himachal Pradesh landslide", "Himachal Pradesh flood",
    "Sikkim flood", "Sikkim landslide",
    "Kerala flood", "Kerala landslide",
    "Assam flood", "Brahmaputra flood",
    "Northeast India flood", "Manipur landslide",
    # Indian agency / operation terms
    "NDRF rescue", "SDRF rescue", "NDMA India", "IMD warning",
    "disaster relief India", "rescue operations India",
    # Infrastructure / impact
    "highway blocked Uttarakhand", "Kedarnath disaster",
    "Chamoli disaster", "Joshimath sinking",
]

# NewsAPI domains restricted to Indian outlets
# Used in queries to filter out non-Indian sources
INDIA_NEWS_DOMAINS = (
    "ndtv.com,thehindu.com,hindustantimes.com,timesofindia.indiatimes.com,"
    "indiatoday.in,aninews.in,news18.com,theprint.in,thewire.in,scroll.in,"
    "deccanherald.com,tribuneindia.com,firstpost.com,outlookindia.com,"
    "dnaindia.com,ndma.gov.in,ddnews.gov.in,pib.gov.in,zeenews.india.com,"
    "newindianexpress.com,telegraphindia.com,livemint.com"
)

# Hostnames allowed for media download — images must come from these Indian domains
INDIA_MEDIA_DOMAINS = {
    "ndtv.com", "thehindu.com", "hindustantimes.com",
    "timesofindia.indiatimes.com", "indiatimes.com",
    "indiatoday.in", "aninews.in", "news18.com",
    "theprint.in", "thewire.in", "scroll.in",
    "deccanherald.com", "tribuneindia.com", "firstpost.com",
    "outlookindia.com", "dnaindia.com", "zeenews.india.com",
    "newindianexpress.com", "telegraphindia.com", "livemint.com",
    "static.ndtv.com", "images.ndtv.com",
    "st1.latestly.com", "img.etimg.com",
    "akm-img-a-in.tosshub.com",         # India Today CDN
    "images.hindustantimes.com",
    "th.thgim.com",                      # The Hindu CDN
    "images.deccanherald.com",
    "img.theweekendleader.com",
    "cdn.downtoearth.org",
    "reliefweb.int", "gdacs.org",        # international but disaster-specific
}

# Monitored locations — hilly / disaster-prone Indian states
MONITORED_LOCATIONS = [
    {"name": "Uttarakhand",     "lat": 30.0668, "lon": 79.0193, "radius_km": 200},
    {"name": "Sikkim",          "lat": 27.5330, "lon": 88.5122, "radius_km": 100},
    {"name": "Kerala",          "lat": 10.8505, "lon": 76.2711, "radius_km": 200},
    {"name": "Himachal Pradesh","lat": 31.1048, "lon": 77.1734, "radius_km": 200},
    {"name": "Assam",           "lat": 26.2006, "lon": 92.9376, "radius_km": 200},
    {"name": "Manipur",         "lat": 24.6637, "lon": 93.9063, "radius_km": 150},
]

# Indian subreddits for disaster / regional news
DISASTER_SUBREDDITS = [
    "india", "IndiaSpeaks", "Uttarakhand", "Kerala",
    "himachal", "sikkim", "assam", "nagaland",
    "indianews", "indiadisasters",
]

# RSS feeds — prioritise Indian government and national news sources
RSS_FEEDS = {
    "ndtv_india":        "https://feeds.feedburner.com/ndtvnews-india-news",
    "thehindu_national": "https://www.thehindu.com/news/national/feeder/default.rss",
    "ndma_india":        "https://ndma.gov.in/rss.xml",
    "downtoearth":       "https://www.downtoearth.org/taxonomy/term/25/all/feed",
    "reliefweb_india":   "https://reliefweb.int/country/ind/rss.xml",
    "gdacs":             "https://www.gdacs.org/xml/rss.xml",
    "hindustan_times":   "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml",
    "india_today":       "https://www.indiatoday.in/rss/home",
    "the_print":         "https://theprint.in/feed/",
}

# Streaming config
STREAM_RECONNECT_WAIT = 5
RATE_LIMIT_PAUSE      = 60

# Batch collection window
BATCH_LOOKBACK_DAYS = 7
