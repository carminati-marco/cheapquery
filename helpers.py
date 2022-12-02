from datetime import datetime as dt


def to_datetime(date):
    return dt.strptime(date, "%Y-%m-%d")


def convert_date(date):
        m = date.month if date.month > 9 else f"0{date.month}"
        d = date.day if date.day > 9 else f"0{date.day}"
        return int(f"{date.year}{m}{d}")

