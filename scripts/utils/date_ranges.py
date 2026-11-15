from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


def previous_complete_date_range(
    timezone_name: str,
    days: int = 10,
    now: datetime | None = None,
) -> tuple[date, date]:
    if days < 1:
        raise ValueError("days must be at least 1")

    zone = ZoneInfo(timezone_name)
    if now is None:
        local_now = datetime.now(zone)
    elif now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    else:
        local_now = now.astimezone(zone)

    end_date = local_now.date() - timedelta(days=1)
    start_date = end_date - timedelta(days=days - 1)
    return start_date, end_date
