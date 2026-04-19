import sys
from loguru import logger
from wp1_data_collection.config.settings import LOG_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)

logger.remove()
logger.add(sys.stdout, level="INFO", format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{name}</cyan> - {message}")
logger.add(LOG_DIR / "wp1_{time:YYYY-MM-DD}.log", rotation="1 day", retention="30 days", level="DEBUG", format="{time} | {level} | {name} - {message}")
