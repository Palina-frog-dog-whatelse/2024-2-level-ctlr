"""
Useful constant variables.
"""
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
ASSETS_PATH = PROJECT_ROOT / "tmp" / "articles"
PROJECT_CONFIG_PATH = PROJECT_ROOT / "project_config.json"

CONFIG_DIR = PROJECT_ROOT / "config"

NUM_ARTICLES_UPPER_LIMIT = 150


CWD = Path(__file__).parent.parent
CRAWLER_CONFIG_PATH = CWD / "lab_5_scraper" / "scraper_config.json"

TIMEOUT_LOWER_LIMIT = 0
TIMEOUT_UPPER_LIMIT = 60
