"""Unit tests for manifest normalization (`plrtool normalize`)."""

import json

import pytest
import yaml
from helpers import parse_cli, raw_plr

import plrtool
from plrtool.exceptions import PlrtoolError
from plrtool.normalize import load_manifest, normalize_plr


def _raw_with_params(params):
    raw = raw_plr("plr-1")
    raw["metadata"].update(
        {
            "uid": "11111111-1111-1111-1111-111111111111",
            "resourceVersion": "42",
            "generation": 3,
            "annotations": {
                "kubectl.kubernetes.io/last-applied-configuration": "{big: json}",
                "keep.example.com/note": "kept",
            },
            "ownerReferences": [{"kind": "Pipeline", "name": "parent", "uid": "deadbeef"}],
        }
    )
    raw["spec"] = {"params": params, "args": ["second", "first"]}
    return raw


def test_normalize_drops_run_specific_data():
    normalized = normalize_plr(_raw_with_params([]))
    assert "status" not in normalized
    assert normalized["metadata"] == {
        "annotations": {"keep.example.com/note": "kept"},
        "name": "plr-1",
        "namespace": "test-rhtap-1-tenant",
    }
    assert normalized["spec"]["args"] == ["second", "first"]


def test_normalize_sorts_keys_and_named_lists():
    raw = _raw_with_params(
        [
            {"name": "zeta", "value": "z"},
            {"name": "alpha", "value": "a"},
        ]
    )
    normalized = normalize_plr(raw)
    assert list(normalized) == ["apiVersion", "kind", "metadata", "spec"]
    assert [param["name"] for param in normalized["spec"]["params"]] == ["alpha", "zeta"]


def test_normalize_params_only():
    normalized = normalize_plr(
        _raw_with_params([{"name": "zeta", "value": "z"}, {"name": "alpha", "value": "a"}]),
        params_only=True,
    )
    assert normalized == [
        {"name": "alpha", "value": "a"},
        {"name": "zeta", "value": "z"},
    ]


def test_load_manifest_reads_yaml_json_and_items_wrapper(tmp_path):
    raw = _raw_with_params([])
    yaml_path = tmp_path / "plr.yaml"
    yaml_path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    assert load_manifest(yaml_path)["metadata"]["name"] == "plr-1"

    json_path = tmp_path / "plr.json"
    json_path.write_text(json.dumps({"items": [raw]}), encoding="utf-8")
    assert load_manifest(json_path)["metadata"]["name"] == "plr-1"


def test_load_manifest_rejects_non_mapping(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("- just\n- a list\n", encoding="utf-8")
    with pytest.raises(PlrtoolError, match="expected a mapping"):
        load_manifest(path)


def test_cmd_normalize_prints_yaml(tmp_path, capsys):
    raw = _raw_with_params([{"name": "b", "value": 2}, {"name": "a", "value": 1}])
    path = tmp_path / "plr.yaml"
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")

    assert plrtool.cmd_normalize(parse_cli(["normalize", str(path)])) == 0
    printed = yaml.safe_load(capsys.readouterr().out)
    assert "status" not in printed
    assert printed["spec"]["params"] == [
        {"name": "a", "value": 1},
        {"name": "b", "value": 2},
    ]

    assert plrtool.cmd_normalize(parse_cli(["normalize", str(path), "--params-only"])) == 0
    printed = yaml.safe_load(capsys.readouterr().out)
    assert printed == [{"name": "a", "value": 1}, {"name": "b", "value": 2}]
