"""Shared data models and helpers."""

from dataclasses import dataclass, asdict
from typing import Optional
from datetime import date


@dataclass
class AircraftSummary:
    registration: str
    make: Optional[str] = None
    model: Optional[str] = None
    last_annual_date: Optional[str] = None
    last_annual_hours: Optional[float] = None
    last_100hr_date: Optional[str] = None
    last_100hr_hours: Optional[float] = None
    last_oil_change_date: Optional[str] = None
    total_time: Optional[float] = None
    upcoming_expirations: list = None  # type: ignore

    def __post_init__(self):
        if self.upcoming_expirations is None:
            self.upcoming_expirations = []

    def to_dict(self):
        return asdict(self)


def _json_serial(obj):
    """JSON serializer for types not handled by default encoder."""
    from decimal import Decimal
    from datetime import datetime, date
    from uuid import UUID
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, UUID):
        return str(obj)
    return str(obj)


def api_response(status_code: int, body: dict) -> dict:
    """Standard API Gateway Lambda proxy response."""
    import json
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
        },
        'body': json.dumps(body, default=_json_serial),
    }
