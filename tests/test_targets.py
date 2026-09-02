"""Unit tests for plrtool target resolution (--namespace/--plr and --csv)."""

import argparse
from pathlib import Path

import plrtool
from plrtool import PlrtoolError, Target

from helpers import parse_cli


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
