"""Unit tests for timing and errors analysis (cache only, no cluster needed)."""

import json
from datetime import UTC

from helpers import raw_plr, raw_pod, raw_taskrun

import plrtool
from plrtool import CacheStore, TimingOptions, run_errors, run_timing


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


def test_run_timing_gantt_pending_running_end_to_end(tmp_path, monkeypatch):
    from plrtool import timing as timing_mod

    captured = []
    monkeypatch.setattr(
        timing_mod,
        "_Gantt",
        type(
            "FakeGantt",
            (),
            {
                "__init__": lambda self: None,
                "add": lambda self, *a: captured.append(a),
                "render": lambda self, *a: None,
            },
        ),
    )
    store = CacheStore(tmp_path)
    store.add_plr(
        raw_plr(
            "ok-1",
            created="2026-08-13T10:00:00Z",
            started="2026-08-13T10:00:10Z",
            completed="2026-08-13T10:00:40Z",
        )
    )
    run_timing(store, TimingOptions(gantt_chart="chart.png"))
    pending = [c for c in captured if c[0] == "PLR ok-1" and c[3] == 0]
    running = [c for c in captured if c[0] == "PLR ok-1" and c[3] == 1]
    # Blue = created -> started, red = started -> completed, both on same row label.
    assert len(pending) == 1 and len(running) == 1
    assert pending[0][1].second == 0  # created
    assert running[0][2].second == 40  # completed
    assert pending[0][2] == running[0][1]  # red starts exactly where blue ends


def test_gantt_render_valid_png(tmp_path):
    from datetime import datetime

    from plrtool.timing import _Gantt

    gantt = _Gantt()
    gantt.add(
        "PLR ok-1",
        datetime(2026, 8, 13, 10, 0, 0, tzinfo=UTC),
        datetime(2026, 8, 13, 10, 0, 10, tzinfo=UTC),
        0,
    )
    gantt.add(
        "PLR ok-1",
        datetime(2026, 8, 13, 10, 0, 10, tzinfo=UTC),
        datetime(2026, 8, 13, 10, 0, 40, tzinfo=UTC),
        1,
    )
    out = tmp_path / "gantt.png"
    gantt.render(str(out))
    assert out.is_file()
    assert out.stat().st_size > 0


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
    # Single PLR => each timestamp range spans zero seconds.
    assert "span=0s" in out
    # --details: per-PR duration header lines are printed.
    run_timing(store, TimingOptions(details=True))
    out = capsys.readouterr().out
    assert "PipelineRun 'ok-1' (" in out
    assert "pending=10" in out
    assert "total=40" in out


def test_run_errors_section_headers_with_counts(tmp_path, capsys):
    store = CacheStore(tmp_path)
    store.add_plr(raw_plr("bad-1", status="False", reason="Failed"))
    store.add_plr(raw_plr("ok-1"))
    store.add_pod(raw_pod("pod-1", phase="Failed"))
    store.add_pod(raw_pod("pod-2", phase="Succeeded"))
    run_errors(store)
    out = capsys.readouterr().out
    # Section titles carry per-section row counts, matching the timing summary.
    assert "=== PipelineRun conditions (1) ===" in out
    assert "=== TaskRun conditions (per task) (0) ===" in out
    assert "=== Pod phases (2) ===" in out
    assert "=== Pod condition failures (status=False) (0) ===" in out


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
