"""Unit tests for plrtool manifest -> record parsing and subsetting."""

from plrtool import parse_plr, parse_pod, parse_taskrun, strip_managed_fields

from helpers import raw_plr, raw_pod, raw_taskrun


def test_parse_plr():
    rec = parse_plr(
        raw_plr(
            "load-test-abc",
            refs=["load-test-abc-task1", "load-test-abc-task2"],
            skipped=2,
            msg="Skipped: 2",
        )
    )
    assert rec.name == "load-test-abc"
    assert rec.namespace == "test-rhtap-1-tenant"
    assert rec.succeeded_status == "True"
    assert rec.tr_refs == ["load-test-abc-task1", "load-test-abc-task2"]
    assert rec.skipped_stopping == 2
    assert rec.completed is not None
    assert rec.succeeded_message == "Skipped: 2"


def test_parse_taskrun():
    rec = parse_taskrun(
        raw_taskrun(
            "load-test-abc-clone",
            pod="pod-x",
            steps=[("clone", "2026-08-13T10:00:10Z", "2026-08-13T10:00:15Z")],
        )
    )
    assert rec.name == "load-test-abc-clone"
    assert rec.pipeline_task == "clone"
    assert rec.pod_name == "pod-x"
    assert len(rec.steps) == 1
    assert rec.steps[0].name == "clone"
    assert rec.steps[0].container == "step-clone"
    assert rec.steps[0].exit_code == 0


def test_parse_pod():
    rec = parse_pod(
        raw_pod(
            "pod-x",
            phase="Failed",
            conds=[("Ready", "False", "PodFailed", "2026-08-13T10:00:30Z")],
            containers=[("build", "Error", "2026-08-13T10:00:12Z", "2026-08-13T10:00:30Z", 1)],
        )
    )
    assert rec.node == "worker-1"
    assert rec.phase == "Failed"
    assert rec.conditions[0] == ("Ready", "False", "PodFailed", "2026-08-13T10:00:30Z")
    assert rec.containers[0].name == "build"
    assert rec.containers[0].terminated_reason == "Error"
    assert rec.containers[0].terminated_exit_code == 1


def test_strip_managed_fields_only_managed_fields():
    raw = {
        "metadata": {"name": "x", "uid": "u-1", "managedFields": [{"a": 1}]},
        "status": {"deep": {"managedFields": {"b": 2}}},
    }
    cleaned = strip_managed_fields(raw)
    assert cleaned["metadata"] == {"name": "x", "uid": "u-1"}
    assert cleaned["status"] == {"deep": {}}
