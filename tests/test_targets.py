"""Unit tests for plrtool target resolution (--namespace/--plr, --csv, time range)."""

import argparse
import datetime as dt
from pathlib import Path

import plrtool
from plrtool import PlrtoolError, Target

from helpers import parse_cli


class FakeListCluster:
    """Cluster stand-in recording list_pipelinerun_names calls."""

    def __init__(self, names):
        self.names = names
        self.calls = []

    def list_pipelinerun_names(self, namespace, since=None, until=None):
        self.calls.append((namespace, since, until))
        return self.names


def test_targets_from_namespace_plr(tmp_path):
    args = parse_cli(
        ["download", "--namespace", "ns-1", "--plr", "plr-1", "--cache", str(tmp_path)]
    )
    assert plrtool.resolve_targets(args) == [Target("ns-1", "plr-1")]


def test_targets_namespace_plr_must_be_together(tmp_path):
    args = parse_cli(["download", "--plr", "plr-1", "--cache", str(tmp_path)])
    try:
        plrtool.resolve_targets(args)
        raise AssertionError("expected PlrtoolError")
    except PlrtoolError:
        pass


def test_targets_from_csv(tmp_path):
    csv = tmp_path / "targets.csv"
    csv.write_text(
        "test-rhtap-1-tenant,load-test-aaa\n"
        "test-rhtap-2-tenant,pipelinerun.tekton.dev/load-test-bbb\n"
        "test-rhtap-3-tenant pipelinerun.tekton.dev/load-test-ccc created\n"
        "# comment\n"
    )
    args = parse_cli(["download", "--csv", str(csv), "--cache", str(tmp_path)])
    assert plrtool.resolve_targets(args) == [
        Target("test-rhtap-1-tenant", "load-test-aaa"),
        Target("test-rhtap-2-tenant", "load-test-bbb"),
        Target("test-rhtap-3-tenant", "load-test-ccc"),
    ]


def test_targets_from_time_range(tmp_path):
    cluster = FakeListCluster(["plr-1", "plr-2"])
    args = parse_cli(
        [
            "download",
            "--namespace",
            "ns-1",
            "--plr-since",
            "2026-08-01",
            "--plr-until",
            "2026-08-13T00:00:00Z",
            "--cache",
            str(tmp_path),
        ]
    )
    assert plrtool.resolve_targets(args, cluster) == [
        Target("ns-1", "plr-1"),
        Target("ns-1", "plr-2"),
    ]
    namespace, since, until = cluster.calls[0]
    assert namespace == "ns-1"
    # date-only bound -> midnight UTC
    assert since == dt.datetime(2026, 8, 1, tzinfo=dt.UTC)
    assert until == dt.datetime(2026, 8, 13, tzinfo=dt.UTC)


def test_time_range_open_ended(tmp_path):
    cluster = FakeListCluster(["plr-1"])
    args = parse_cli(
        ["download", "--namespace", "ns-1", "--plr-since", "2026-08-01", "--cache", str(tmp_path)]
    )
    assert plrtool.resolve_targets(args, cluster) == [Target("ns-1", "plr-1")]
    namespace, since, until = cluster.calls[0]
    assert since == dt.datetime(2026, 8, 1, tzinfo=dt.UTC)
    assert until is None


def test_time_range_combines_with_plr(tmp_path):
    cluster = FakeListCluster(["plr-2"])
    args = parse_cli(
        [
            "download",
            "--namespace",
            "ns-1",
            "--plr",
            "plr-1",
            "--plr-since",
            "2026-08-01",
            "--cache",
            str(tmp_path),
        ]
    )
    assert plrtool.resolve_targets(args, cluster) == [
        Target("ns-1", "plr-1"),
        Target("ns-1", "plr-2"),
    ]


def test_time_range_requires_namespace(tmp_path):
    args = parse_cli(["download", "--plr-since", "2026-08-01", "--cache", str(tmp_path)])
    try:
        plrtool.resolve_targets(args, FakeListCluster([]))
        raise AssertionError("expected PlrtoolError")
    except PlrtoolError as exc:
        assert "--namespace" in str(exc)


def test_time_range_requires_cluster(tmp_path):
    args = parse_cli(
        ["download", "--namespace", "ns-1", "--plr-since", "2026-08-01", "--cache", str(tmp_path)]
    )
    try:
        plrtool.resolve_targets(args)
        raise AssertionError("expected PlrtoolError")
    except PlrtoolError as exc:
        assert "cluster" in str(exc)


def test_time_range_rejects_bad_date(tmp_path):
    args = parse_cli(
        ["download", "--namespace", "ns-1", "--plr-since", "not-a-date", "--cache", str(tmp_path)]
    )
    try:
        plrtool.resolve_targets(args, FakeListCluster([]))
        raise AssertionError("expected PlrtoolError")
    except PlrtoolError as exc:
        assert "not-a-date" in str(exc)


def test_time_range_rejects_reversed_bounds(tmp_path):
    args = parse_cli(
        [
            "download",
            "--namespace",
            "ns-1",
            "--plr-since",
            "2026-08-13",
            "--plr-until",
            "2026-08-01",
            "--cache",
            str(tmp_path),
        ]
    )
    try:
        plrtool.resolve_targets(args, FakeListCluster([]))
        raise AssertionError("expected PlrtoolError")
    except PlrtoolError as exc:
        assert "before" in str(exc)


def test_time_range_empty_result_is_error(tmp_path):
    args = parse_cli(
        [
            "download",
            "--namespace",
            "ns-1",
            "--plr-since",
            "2026-08-01",
            "--plr-until",
            "2026-08-02",
            "--cache",
            str(tmp_path),
        ]
    )
    try:
        plrtool.resolve_targets(args, FakeListCluster([]))
        raise AssertionError("expected PlrtoolError")
    except PlrtoolError as exc:
        assert "no PipelineRuns found" in str(exc)


def test_targets_deduplicated_and_required():
    # order preserved, duplicates merged
    csv = Path("/tmp") / "plrtool-test-dupes.csv"
    csv.write_text("ns-1,plr-a\nns-1,plr-a\nns-2,plr-b\n")
    try:
        args = parse_cli(["download", "--csv", str(csv)])
        assert plrtool.resolve_targets(args) == [Target("ns-1", "plr-a"), Target("ns-2", "plr-b")]
    finally:
        csv.unlink(missing_ok=True)
    # no targets -> error
    try:
        plrtool.resolve_targets(argparse.Namespace(namespace=None, plr=None, csv=None))
        raise AssertionError("expected PlrtoolError")
    except PlrtoolError:
        pass
