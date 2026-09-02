"""Unit tests for plrtool time/stat utilities: ts parsing, formatting, durations, percentiles."""

from plrtool import (
    duration_seconds,
    epoch_of,
    fmt_ts,
    normalize_message,
    parse_duration,
    percentile,
    parse_ts_dt,
    PlrtoolError,
)


def test_parse_ts_dt():
    parsed = parse_ts_dt("2026-08-13T10:00:00Z")
    assert parsed is not None
    assert parsed.tzinfo is not None
    assert parse_ts_dt(None) is None
    assert parse_ts_dt("n/a") is None
    assert parse_ts_dt("null") is None
    assert parse_ts_dt("garbage") is None
    # offset is normalized to UTC
    assert parse_ts_dt("2026-08-13T12:00:00+02:00").hour == 10


def test_fmt_ts_and_epoch():
    value = parse_ts_dt("2026-08-13T10:00:05Z")
    assert fmt_ts(value) == "2026-08-13T10:00:05Z"
    assert fmt_ts(None) == "n/a"
    assert epoch_of(value) == int(value.timestamp())
    assert epoch_of(None) is None


def test_duration_seconds():
    start = parse_ts_dt("2026-08-13T10:00:00Z")
    end = parse_ts_dt("2026-08-13T10:05:00Z")
    assert duration_seconds(start, end) == 300
    assert duration_seconds(None, end) is None
    assert duration_seconds(start, None) is None


def test_percentile_matches_original():
    # The original bash computed p99 = sorted[ceil(0.99*n)-1]; n=100 -> rank 99.
    values = list(range(1, 101))
    assert percentile(values, 99) == 99
    assert percentile([5, 1, 3], 99) == 5
    assert percentile([], 99) is None


def test_parse_duration():
    assert parse_duration("30s") == 30.0
    assert parse_duration("100m") == 6000.0
    assert parse_duration("2h") == 7200.0
    assert parse_duration("1h30m") == 5400.0
    try:
        parse_duration("bogus")
        raise AssertionError("expected PlrtoolError")
    except PlrtoolError:
        pass


def test_normalize_message():
    assert normalize_message(None) == "missing"
    assert normalize_message("failed in test-rhtap-12-tenant") == "failed in test-rhtap-...-tenant"
    assert normalize_message("load-test-123-abcd pod") == "load-test-... pod"
    assert normalize_message("uid deadbeef1234567890abc123 here") == "uid ... here"
