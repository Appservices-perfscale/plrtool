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


def test_run_timing_plr_name_filters_to_single_plr(tmp_path):
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
    summary = tmp_path / "summary.json"
    exit_code = run_timing(store, TimingOptions(summary=str(summary), plr_name="ok-1"))
    assert exit_code == 0
    document = json.loads(summary.read_text(encoding="utf-8"))
    assert document["Succeeded"]["total"] == 1
    assert document["pending"]["min"] == 10
    assert document["pending"]["max"] == 10


def test_run_timing_plr_name_not_found(tmp_path):
    store = CacheStore(tmp_path)
    store.add_plr(raw_plr("ok-1"))
    assert run_timing(store, TimingOptions(plr_name="nope")) == 1


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
            refs=("ok-1-build",),
        )
    )
    store.add_taskrun(
        raw_taskrun(
            "ok-1-build",
            created="2026-08-13T10:00:05Z",
            started="2026-08-13T10:00:10Z",
            completed="2026-08-13T10:00:40Z",
            steps=(("build", "2026-08-13T10:00:15Z", "2026-08-13T10:00:35Z"),),
        )
    )
    store.load()  # link the TaskRun onto the PLR record
    run_timing(store, TimingOptions(gantt_chart="chart.png"))
    pending = [c for c in captured if c[0] == "PLR ok-1" and c[3] == 0]
    running = [c for c in captured if c[0] == "PLR ok-1" and c[3] == 1]
    # Blue = created -> started, red = started -> completed, both on same row label.
    assert len(pending) == 1 and len(running) == 1
    assert pending[0][1].second == 0  # created
    assert running[0][2].second == 40  # completed
    assert pending[0][2] == running[0][1]  # red starts exactly where blue ends
    # TaskRun runs in a darker red than the PLR, the step darker still.
    tr_pending = [c for c in captured if c[0] == "TR build" and c[3] == 0]
    tr_running = [c for c in captured if c[0] == "TR build" and c[3] == 2]
    step = [c for c in captured if c[0] == "Step build" and c[3] == 3]
    assert len(tr_pending) == 1 and len(tr_running) == 1 and len(step) == 1


def test_run_timing_gantt_flags_drop_trs_and_steps(tmp_path, monkeypatch):
    """--gantt-no-taskruns / --gantt-no-steps keep only PLR bars in the chart."""
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
            refs=["ok-1-build"],
        )
    )
    store.add_taskrun(
        raw_taskrun(
            "ok-1-build",
            steps=[("build", "2026-08-13T10:00:15Z", "2026-08-13T10:00:35Z")],
        )
    )
    store.load()
    run_timing(store, TimingOptions(gantt_chart="chart.png"))
    assert [c[0] for c in captured] == [
        "PLR ok-1",
        "PLR ok-1",
        "TR build",
        "TR build",
        "Step build",
    ]

    captured.clear()
    run_timing(store, TimingOptions(gantt_chart="chart.png", gantt_steps=False))
    assert [c[0] for c in captured] == ["PLR ok-1", "PLR ok-1", "TR build", "TR build"]

    captured.clear()
    run_timing(store, TimingOptions(gantt_chart="chart.png", gantt_taskruns=False))
    assert [c[0] for c in captured] == ["PLR ok-1", "PLR ok-1"]


def test_run_timing_gantt_orders_plr_rows_by_start(tmp_path, monkeypatch):
    """PLR rows on the Gantt Y-axis follow startTime, not creation order."""
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
    # ok-1 is created first but starts later than ok-2.
    store.add_plr(
        raw_plr(
            "ok-1",
            created="2026-08-13T10:00:00Z",
            started="2026-08-13T10:00:30Z",
            completed="2026-08-13T10:01:00Z",
        )
    )
    store.add_plr(
        raw_plr(
            "ok-2",
            created="2026-08-13T10:00:10Z",
            started="2026-08-13T10:00:20Z",
            completed="2026-08-13T10:00:50Z",
        )
    )
    run_timing(store, TimingOptions(gantt_chart="chart.png"))
    # Row order = first add per label (y=0 at top after invert_yaxis).
    seen: list = []
    seen_set = set()
    for label, *_rest in captured:
        if label.startswith("PLR ") and label not in seen_set:
            seen_set.add(label)
            seen.append(label)
    assert seen == ["PLR ok-2", "PLR ok-1"]


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


def test_gantt_rows_group_repeated_labels_under_parent():
    """Same label far apart gets its own row, so rows nest PLR -> TR -> steps."""
    from datetime import datetime

    from plrtool.timing import _Gantt

    gantt = _Gantt()
    t0 = datetime(2026, 8, 13, 10, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 8, 13, 10, 1, 0, tzinfo=UTC)
    for label in ("PLR a", "TR x", "Step clone", "PLR b", "TR x", "Step clone"):
        gantt.add(label, t0, t1, 1)
    # Consecutive same-label adds still merge into one row.
    gantt.add("Step clone", t0, t1, 3)
    assert [label for label, _ in gantt.rows] == [
        "PLR a",
        "TR x",
        "Step clone",
        "PLR b",
        "TR x",
        "Step clone",
    ]
    assert len(gantt.rows[5][1]) == 2  # consecutive Step adds merged
    assert len(gantt.rows[2][1]) == 1  # non-consecutive ones stay separate rows


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


def test_run_timing_details_json_writes_per_plr_document(tmp_path, capsys):
    """--details-json dumps the details investigation as a PLR-keyed dict, no --details needed."""
    store = CacheStore(tmp_path)
    store.add_plr(
        raw_plr(
            "ok-1",
            created="2026-08-13T10:00:00Z",
            started="2026-08-13T10:00:10Z",
            completed="2026-08-13T10:00:40Z",
            refs=["ok-1-task"],
        )
    )
    store.add_taskrun(
        raw_taskrun(
            "ok-1-task",
            pod="pod-1",
            steps=[("clone", "2026-08-13T10:00:11Z", "2026-08-13T10:00:14Z")],
        )
    )
    store.add_pod(
        raw_pod(
            "pod-1",
            conds=[("PodScheduled", "True", "-", "2026-08-13T10:00:09Z")],
            containers=[
                ("prepare", "Completed", "2026-08-13T10:00:11Z", "2026-08-13T10:00:12Z", 0)
            ],
        )
    )
    store.load()  # link TaskRun/Pod onto the PLR record
    out_file = tmp_path / "details.json"
    exit_code = run_timing(store, TimingOptions(details_json=str(out_file)))
    assert exit_code == 0
    # No --details flag => no per-PLR printout, only the file write message.
    out = capsys.readouterr().out
    assert "PipelineRun 'ok-1' (" not in out
    assert "Wrote PLR details to" in out
    document = json.loads(out_file.read_text(encoding="utf-8"))
    assert list(document) == ["ok-1"]
    plr = document["ok-1"]
    assert plr["name"] == "ok-1"
    assert plr["created"] == "2026-08-13T10:00:00+00:00"
    assert plr["pending"] == 10
    assert plr["running"] == 30
    assert plr["total"] == 40
    assert plr["plr_wait_time"] == 6  # PLR created 10:00:00, TR created 10:00:06
    tr = plr["taskruns"][0]
    assert tr["name"] == "ok-1-task"
    assert tr["tr_wait_time"] == 5  # TR created 10:00:06, first step start 10:00:11
    assert tr["steps"][0]["name"] == "clone"
    assert tr["steps"][0]["duration"] == 3
    assert tr["pod"]["containers"][0]["duration"] == 1


def test_run_timing_details_labels_step_and_pod_sections(tmp_path, capsys):
    """--details labels the step list and the pod condition/container blocks."""
    store = CacheStore(tmp_path)
    store.add_plr(
        raw_plr(
            "ok-1",
            created="2026-08-13T10:00:00Z",
            started="2026-08-13T10:00:10Z",
            completed="2026-08-13T10:00:40Z",
            refs=["ok-1-task"],
        )
    )
    store.add_taskrun(
        raw_taskrun(
            "ok-1-task",
            pod="pod-1",
            steps=[("clone", "2026-08-13T10:00:11Z", "2026-08-13T10:00:14Z")],
        )
    )
    store.add_pod(
        raw_pod(
            "pod-1",
            conds=[("PodScheduled", "True", "-", "2026-08-13T10:00:09Z")],
            containers=[
                ("prepare", "Completed", "2026-08-13T10:00:11Z", "2026-08-13T10:00:12Z", 0)
            ],
        )
    )
    store.load()  # link TaskRun/Pod onto the PLR record
    run_timing(store, TimingOptions(details=True))
    out = capsys.readouterr().out
    assert "TaskRun steps:" in out
    assert "Pod conditions:" in out
    assert "Pod container durations:" in out


def test_run_timing_histograms(tmp_path, monkeypatch):
    from datetime import datetime

    captured = []

    def fake_render(values, buckets, title, xlabel, output, datetime_x=False):
        captured.append((values, buckets, title, datetime_x))

    monkeypatch.setattr("plrtool.timing._render_histogram", fake_render)
    store = CacheStore(tmp_path)
    store.add_plr(
        raw_plr(
            "ok-1",
            created="2026-08-13T10:00:00Z",
            started="2026-08-13T10:00:10Z",
            completed="2026-08-13T10:00:40Z",
        )
    )
    # Failed PLR has a start time too: must feed the start histogram.
    store.add_plr(
        raw_plr(
            "bad-1",
            status="False",
            reason="Failed",
            started="2026-08-13T10:00:20Z",
            completed="2026-08-13T10:01:00Z",
        )
    )
    run_timing(
        store,
        TimingOptions(
            start_histogram="starts.png",
            duration_histogram="durations.png",
            histogram_buckets=7,
        ),
    )
    starts, durations = captured
    # Start histogram: every cached PLR with a startTime, custom bucket count.
    assert starts[1] == 7 and starts[3] is True
    assert [v.second for v in starts[0]] == [10, 20]
    assert starts[0][1] == datetime(2026, 8, 13, 10, 0, 20, tzinfo=UTC)
    # Duration histogram: succeeded PLRs only, running seconds (startTime -> completion).
    assert durations[1] == 7 and durations[3] is False
    assert durations[0] == [30]


def test_render_histogram_valid_png(tmp_path):
    from plrtool.timing import _render_histogram

    out = tmp_path / "hist.png"
    _render_histogram([1, 2, 2, 3], 2, "t", "x", str(out))
    assert out.is_file()
    assert out.stat().st_size > 0
    # No data -> no file, just a skip message.
    skipped = tmp_path / "empty.png"
    _render_histogram([], 2, "t", "x", str(skipped))
    assert not skipped.exists()


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
