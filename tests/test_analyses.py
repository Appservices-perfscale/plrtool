"""Unit tests for timing and errors analysis (cache only, no cluster needed)."""

import json

import plrtool
from plrtool import CacheStore, TimingOptions, run_errors, run_timing

from helpers import raw_pod, raw_plr, raw_taskrun


def test_run_timing_only_succeeded_and_summary(tmp_path):
    store = CacheStore(tmp_path)
    store.add_plr(
        raw_plr(
            "ok-1",
            created="2026-08-13T10:00:00Z",
            started="2026-08-13T10:00:10Z",
            completed="2026-08-13T10:00:40Z",
        )
    )
    store.add_plr(
        raw_plr(
            "ok-2",
            created="2026-08-13T10:00:20Z",
            started="2026-08-13T10:00:40Z",
            completed="2026-08-13T10:01:00Z",
        )
    )
    store.add_plr(raw_plr("bad-1", status="False", reason="Failed"))
    summary = tmp_path / "summary.json"
    exit_code = run_timing(store, TimingOptions(summary=str(summary)))
    assert exit_code == 0
    assert summary.is_file()
    document = json.loads(summary.read_text(encoding="utf-8"))
    # Only the 2 succeeded PLRs contribute; grand totals reflect both.
    assert document["Succeeded"]["total"] == 2
    assert document["Succeeded"]["True"] == 2
    assert len(document["pending"]["data"]) == 2
    assert document["pending"]["min"] == 10
    assert document["pending"]["max"] == 20
    assert document["total"]["max"] == 40
    assert document["total"]["avg"] == 40


def test_run_timing_empty_store(tmp_path):
    assert run_timing(CacheStore(tmp_path), TimingOptions()) == 0


def test_run_timing_per_pr_detail_gated_by_details(tmp_path, capsys):
    store = CacheStore(tmp_path)
    store.add_plr(
        raw_plr(
            "ok-1",
            created="2026-08-13T10:00:00Z",
            started="2026-08-13T10:00:10Z",
            completed="2026-08-13T10:00:40Z",
        )
    )
    # Default: summary and totals only, no per-PR header lines.
    run_timing(store, TimingOptions())
    out = capsys.readouterr().out
    assert "PipelineRun 'ok-1'" not in out
    assert "=== Summary" in out
    # --details: per-PR duration header lines are printed.
    run_timing(store, TimingOptions(details=True))
    out = capsys.readouterr().out
    assert "PipelineRun 'ok-1' (" in out
    assert "pending=10" in out
    assert "total=40" in out


def test_run_errors_histograms_and_classification(tmp_path):
    store = CacheStore(tmp_path)
    store.add_plr(
        raw_plr(
            "bad-1",
            status="False",
            reason="Failed",
            refs=["bad-1-task"],
            msg="failed in load-test-123-pod",
        )
    )
    store.add_plr(raw_plr("ok-1"))
    store.add_taskrun(
        raw_taskrun(
            "bad-1-task", status="False", reason="TaskRunImagePullFailed", msg="image pull error"
        )
    )
    store.add_pod(raw_pod("pod-1", phase="Failed"))
    assert run_errors(store) == 0
    # Successful PLR/TR do not appear in the failure histograms.
    assert plrtool.collect_plr_conditions(store.plrs.values()) == {
        ("False", "Failed", "failed in load-test-..."): 1
    }
    assert plrtool.collect_taskrun_conditions(store.taskruns.values()) == {
        "task": {("False", "TaskRunImagePullFailed", "image pull error"): 1}
    }


def test_classify_canceled_and_oom(tmp_path, capsys):
    store = CacheStore(tmp_path)
    store.add_plr(raw_plr("c1", status="False", reason="Cancelled"))
    store.add_plr(raw_plr("c2", status="False", reason="Failed", refs=["c2-task"]))
    store.add_taskrun(
        raw_taskrun(
            "c2-task", status="False", reason="Failed", msg="step exited with code 137: OOMKilled"
        )
    )
    store.load()  # (re)link TaskRuns onto the PLR records via childReferences
    plrtool.classify_failures(store.plrs.values())
    out = capsys.readouterr().out
    assert "was cancelled" in out
    assert "OOMKilled" in out
