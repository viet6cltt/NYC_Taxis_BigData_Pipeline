from datetime import date, datetime
from decimal import Decimal 

def normalize_value(value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    try:
        if hasattr(value, 'item'):
            return value.item()
    except Exception:
        pass
    return value


def normalize_row(row_dict: dict) -> dict:
    return {k: normalize_value(v) for k, v in row_dict.items()}