# -*- coding: utf-8 -*-
"""Shared GTI reporting window.

The daily report run at 05:00 KST covers the half-open interval from the
previous day's 05:00 through the current day's 05:00.  A delayed/manual run
after 05:00 keeps the same completed window; a run before 05:00 uses the
window ending at that day's 05:00.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable

import pandas as pd


KST = timezone(timedelta(hours=9), name="KST")
REPORT_CUTOFF_HOUR = 5


def kst_now() -> datetime:
    return datetime.now(timezone.utc).astimezone(KST)


def report_window(now: datetime | pd.Timestamp | None = None):
    ref = pd.Timestamp(now if now is not None else kst_now())
    if ref.tzinfo is None:
        ref = ref.tz_localize(KST)
    else:
        ref = ref.tz_convert(KST)
    end = ref.normalize() + pd.Timedelta(hours=REPORT_CUTOFF_HOUR)
    if ref < end:
        end -= pd.Timedelta(days=1)
    start = end - pd.Timedelta(days=1)
    return start, end


def _one_to_kst(value):
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        return ts.tz_localize(KST)
    return ts.tz_convert(KST)


def previous_kst_day_mask(values: Iterable, now=None):
    series = values if isinstance(values, pd.Series) else pd.Series(values)
    aware = series.map(_one_to_kst)
    start, end = report_window(now)
    mask = aware.notna() & aware.ge(start) & aware.lt(end)
    # Existing GTI workbooks store KST wall-clock values without timezone.
    # Return that representation for Excel compatibility while the mask is
    # calculated with timezone-aware values.
    normalized = aware.map(
        lambda x: pd.NaT if pd.isna(x) else x.tz_localize(None)
    )
    normalized.index = series.index
    mask.index = series.index
    return mask, normalized, start, end
