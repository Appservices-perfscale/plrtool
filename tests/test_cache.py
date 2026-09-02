"""Unit tests for CacheStore: JSON dump/load and legacy YAML reading."""

import json

import yaml

import plrtool
from plrtool import CacheStore

from helpers import raw_pod, raw_plr, raw_taskrun


def test_cache_store_dump_strips_managed_fields(tmp_path):
    store = CacheStore(tmp_path)
    store.add_plr(raw_plr("plr-1"))
    path = tmp_path / "collected-pipelinerun-plr-1.json"
    assert path.is_file()
    dumped = json.loads(path.read_text(encoding="utf-8"))
    assert "managedFields" not in dumped["metadata"]
    assert dumped["metadata"]["name"] == "plr-1"


def test_cache_store_load_roundtrip(tmp_path):
    store = CacheStore(tmp_path)
    store.add_plr(raw_plr("plr-1", refs=["plr-1-task"]))
    store.add_taskrun(raw_taskrun("plr-1-task", pod="pod-1"))
    store.add_pod(raw_pod("pod-1"))
    reloaded = CacheStore(tmp_path).load()
    assert set(reloaded.plrs) == {"plr-1"}
    assert set(reloaded.taskruns) == {"plr-1-task"}
    assert set(reloaded.pods) == {"pod-1"}
    plr = reloaded.plrs["plr-1"]
    assert len(plr.taskruns) == 1
    assert plr.taskruns[0].pipeline_task == "task"
    assert plr.taskruns[0].pod is reloaded.pods["pod-1"]


def test_cache_store_load_legacy_yaml(tmp_path):
    # Old run directories store {items:[...]} YAML; JSON twin wins when both exist.
    (tmp_path / "collected-pipelinerun-legacy.yaml").write_text(
        yaml.safe_dump({"apiVersion": "v1", "items": [raw_plr("legacy")]}), encoding="utf-8"
    )
    store = CacheStore(tmp_path).load()
    assert set(store.plrs) == {"legacy"}

    # Same object as JSON: only the JSON twin is parsed (no duplicate records).
    store.add_plr(raw_plr("legacy"))
    reloaded = CacheStore(tmp_path).load()
    assert set(reloaded.plrs) == {"legacy"}


def test_load_doc_unwraps_items_fallback(tmp_path):
    # Wrapper is the oc/kubectl {items:[...]} shape; items[0] is unwrapped.
    path = tmp_path / "collected-pipelinerun-wrapped.yaml"
    path.write_text(
        yaml.safe_dump({"apiVersion": "v1", "items": [raw_plr("wrapped")]}), encoding="utf-8"
    )
    doc = plrtool.cache._load_doc(path)
    assert doc["metadata"]["name"] == "wrapped"


def test_load_doc_warns_on_multiple_items(tmp_path, caplog):
    path = tmp_path / "collected-pipelinerun-multi.yaml"
    path.write_text(yaml.safe_dump({"items": [raw_plr("a"), raw_plr("b")]}), encoding="utf-8")
    with caplog.at_level("WARNING"):
        doc = plrtool.cache._load_doc(path)
    assert doc["metadata"]["name"] == "a"  # only the first item is used
    assert any("using items[0] only" in record.message for record in caplog.records)


def test_load_doc_empty_items_is_missing(tmp_path, caplog):
    path = tmp_path / "collected-pipelinerun-empty.yaml"
    path.write_text(yaml.safe_dump({"items": []}), encoding="utf-8")
    with caplog.at_level("WARNING"):
        assert plrtool.cache._load_doc(path) is None
    assert any("empty 'items' list" in record.message for record in caplog.records)
