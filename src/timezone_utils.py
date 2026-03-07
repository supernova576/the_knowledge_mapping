from datetime import datetime
from zoneinfo import ZoneInfo


ZURICH_TIMEZONE = ZoneInfo("Europe/Zurich")


def now_in_zurich() -> datetime:
    return datetime.now(ZURICH_TIMEZONE)


def now_in_zurich_str() -> str:
    return now_in_zurich().strftime("%Y-%m-%d %H:%M:%S")
