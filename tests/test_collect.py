"""Unit tests for the collect/wait helpers driven by a stub cluster."""

import yaml
from helpers import StubCluster, raw_plr, raw_pod, raw_taskrun

import plrtool
from plrtool import CacheStore, DownloadOptions, Target


def test_collect_plr_manifest_states(tmp_path):
    store = CacheStore(tmp_path)
    stub = StubCluster(
        objects={
            ("pipelinerun", "ns-1", "done-plr"): raw_plr("done-plr"),
            ("pipelinerun", "ns-1", "running-plr"): raw_plr(
                "running-plr", status="Unknown", reason="Running"
            ),
        }
    )
    result, rec = plrtool.collect_plr_manifest(store, stub, Target("ns-1", "done-plr"))
    assert result == "dumped"
    assert rec.succeeded_status == "True"

    result, rec = plrtool.collect_plr_manifest(store, stub, Target("ns-1", "running-plr"))
    assert result == "incomplete"
    assert rec is None

    result, rec = plrtool.collect_plr_manifest(store, stub, Target("ns-1", "missing-plr"))
    assert result == "missing"


def test_collect_one_fetches_details_only_when_requested(tmp_path):
    store = CacheStore(tmp_path)
    stub = StubCluster(
        objects={
            ("pipelinerun", "ns-1", "plr-1"): raw_plr("plr-1", refs=["plr-1-task"]),
            ("taskrun", "ns-1", "plr-1-task"): raw_taskrun(
                "plr-1-task",
                pod="pod-1",
                steps=[("clone", "2026-08-13T10:00:10Z", "2026-08-13T10:00:15Z")],
            ),
            ("pod", "ns-1", "pod-1"): raw_pod("pod-1"),
        },
        logs={("pod-1", "step-clone"): "hello log"},
    )
    # Default: PLR only, no details.
    options = DownloadOptions(cache_dir=tmp_path)
    assert plrtool.collect_one(store, stub, options, Target("ns-1", "plr-1")) is True
    assert set(store.taskruns) == set()
    assert not (tmp_path / "collected-taskrun-plr-1-task.json").exists()

    # --details-included: TR + pod + log fetched and dumped.
    options = DownloadOptions(cache_dir=tmp_path, details_included=True)
    assert plrtool.collect_one(store, stub, options, Target("ns-1", "plr-1")) is True
    assert set(store.taskruns) == {"plr-1-task"}
    assert set(store.pods) == {"pod-1"}
    assert (tmp_path / "pod-pod-1-step-clone.log").read_text() == "hello log"
    plr = store.plrs["plr-1"]
    assert plr.taskruns[0].pod is store.pods["pod-1"]


def test_collect_one_failed_details(tmp_path):
    # --details-if-failed only fetches details for finished-failed PLRs.
    store = CacheStore(tmp_path)
    stale = CacheStore(tmp_path)
    failed = raw_plr("bad-plr", status="False", reason="Failed", refs=["bad-plr-task"])
    stub = StubCluster(
        objects={
            ("pipelinerun", "ns-1", "bad-plr"): failed,
            ("taskrun", "ns-1", "bad-plr-task"): raw_taskrun("bad-plr-task", pod="pod-b"),
        }
    )
    options = DownloadOptions(cache_dir=tmp_path, details_if_failed=True)
    assert plrtool.collect_one(stale, stub, options, Target("ns-1", "bad-plr")) is True
    assert set(store.taskruns) == set()
    assert set(stale.taskruns) == {"bad-plr-task"}


def test_collect_plr_manifest_cached_wins(tmp_path):
    # Cached file wins: no cluster access needed for an already-collected PLR.
    store = CacheStore(tmp_path)
    store.add_plr(raw_plr("plr-cached"))
    stub = StubCluster(objects={})  # cluster reports nothing
    result, rec = plrtool.collect_plr_manifest(store, stub, Target("ns-1", "plr-cached"))
    assert result == "dumped"
    assert rec.succeeded_status == "True"
    assert rec.name == "plr-cached"


def test_collect_plr_manifest_uses_legacy_yaml_cache(tmp_path):
    # Legacy {items:[...]} YAML cache is reused too (migrates to a JSON twin).
    store = CacheStore(tmp_path)
    (tmp_path / "collected-pipelinerun-legacy.yaml").write_text(
        yaml.safe_dump({"items": [raw_plr("legacy")]}), encoding="utf-8"
    )
    stub = StubCluster(objects={})
    result, rec = plrtool.collect_plr_manifest(store, stub, Target("ns-1", "legacy"))
    assert result == "dumped"
    assert rec.name == "legacy"
    assert (tmp_path / "collected-pipelinerun-legacy.json").is_file()


def test_fetch_details_reuses_cached_artifacts(tmp_path):
    # TaskRun/Pod/log files already in cache are reused (no cluster access).
    store = CacheStore(tmp_path)
    store.add_plr(raw_plr("p", refs=["p-task"]))
    store.add_taskrun(
        raw_taskrun(
            "p-task",
            pod="pod-x",
            steps=[("a", "2026-08-13T10:00:10Z", "2026-08-13T10:00:15Z")],
        )
    )
    store.add_pod(raw_pod("pod-x"))
    store.add_log("pod-x", "step-a", "cached log")
    stub = StubCluster(objects={}, logs={})
    plrtool.download._fetch_details(store, stub, store.plrs["p"], Target("ns-1", "p"))
    assert store.taskruns["p-task"].pod is store.pods["pod-x"]
    assert (tmp_path / "pod-pod-x-step-a.log").read_text() == "cached log"


def test_missing_logs_are_counted_and_reported(tmp_path, caplog):
    # Pod manifest is archived but the container log is unavailable (pod gone
    # from the live cluster) -> collect succeeds but warns about the gap.
    store = CacheStore(tmp_path)
    stub = StubCluster(
        objects={
            ("pipelinerun", "ns-1", "plr-1"): raw_plr("plr-1", refs=["plr-1-task"]),
            ("taskrun", "ns-1", "plr-1-task"): raw_taskrun(
                "plr-1-task",
                pod="pod-1",
                steps=[("a", "2026-08-13T10:00:10Z", "2026-08-13T10:00:15Z")],
            ),
            ("pod", "ns-1", "pod-1"): raw_pod("pod-1"),
        },
        logs={},
    )
    options = DownloadOptions(cache_dir=tmp_path, details_included=True)
    with caplog.at_level("WARNING", logger="plrtool"):
        assert plrtool.collect_one(store, stub, options, Target("ns-1", "plr-1")) is True
    assert "1 container log(s) not downloaded" in caplog.text
    assert not list(tmp_path.glob("pod-*.log"))


def test_missing_log_count_aggregates_containers(tmp_path, caplog):
    # Two step containers, neither retrievable -> count of 2 in the warning.
    store = CacheStore(tmp_path)
    stub = StubCluster(
        objects={
            ("pipelinerun", "ns-1", "plr-1"): raw_plr("plr-1", refs=["plr-1-task"]),
            ("taskrun", "ns-1", "plr-1-task"): raw_taskrun(
                "plr-1-task",
                pod="pod-1",
                steps=[
                    ("a", "2026-08-13T10:00:10Z", "2026-08-13T10:00:15Z"),
                    ("b", "2026-08-13T10:00:10Z", "2026-08-13T10:00:15Z"),
                ],
            ),
            ("pod", "ns-1", "pod-1"): raw_pod("pod-1"),
        },
        logs={},
    )
    options = DownloadOptions(cache_dir=tmp_path, details_included=True)
    with caplog.at_level("WARNING", logger="plrtool"):
        plrtool.collect_one(store, stub, options, Target("ns-1", "plr-1"))
    assert "2 container log(s) not downloaded" in caplog.text


def test_taskrun_with_no_step_containers_warns(tmp_path, caplog):
    # TaskRun status has no step container names -> logs cannot be derived.
    store = CacheStore(tmp_path)
    stub = StubCluster(
        objects={
            ("pipelinerun", "ns-1", "plr-1"): raw_plr("plr-1", refs=["plr-1-task"]),
            ("taskrun", "ns-1", "plr-1-task"): raw_taskrun("plr-1-task", pod="pod-1"),
            ("pod", "ns-1", "pod-1"): raw_pod("pod-1"),
        }
    )
    options = DownloadOptions(cache_dir=tmp_path, details_included=True)
    with caplog.at_level("WARNING", logger="plrtool"):
        plrtool.collect_one(store, stub, options, Target("ns-1", "plr-1"))
    assert "no container names in status" in caplog.text
    assert "1 container log(s) not downloaded" in caplog.text
