"""Unit tests for plrtool (single test file, no cluster needed).

Covers the pure logic (parsing, stats, normalizing, targets, cache load/dump)
plus the shared collect/wait helpers with a stubbed cluster.  Cluster API and
matplotlib rendering paths are exercised only with fakes / real cache data.
"""

import argparse
import json
from pathlib import Path

import yaml

import plrtool
from plrtool import (
    CacheStore,
    DownloadOptions,
    PlrtoolError,
    Target,
    TimingOptions,
    duration_seconds,
    epoch_of,
    fmt_ts,
    normalize_message,
    parse_duration,
    parse_plr,
    parse_pod,
    parse_taskrun,
    parse_ts_dt,
    percentile,
    run_errors,
    run_timing,
    strip_managed_fields,
)

# ---------------------------------------------------------------------------
# Helpers to build fake raw manifests
# ---------------------------------------------------------------------------


def raw_plr(
    name,
    ns="test-rhtap-1-tenant",
    status="True",
    reason="Succeeded",
    created="2026-08-13T10:00:00Z",
    started="2026-08-13T10:00:05Z",
    completed="2026-08-13T10:05:00Z",
    refs=(),
    skipped=0,
    msg=None,
    managed_fields=True,
):
    status_dict = {
        "startTime": started,
        "completionTime": completed,
        "conditions": [{"type": "Succeeded", "status": status, "reason": reason, "message": msg}],
    }
    if refs:
        status_dict["childReferences"] = [
            {"apiVersion": "tekton.dev/v1", "kind": "TaskRun", "name": ref} for ref in refs
        ]
    if skipped:
        status_dict["skippedTasks"] = [
            {"name": f"t{i}", "reason": "PipelineRun was stopping"} for i in range(skipped)
        ]
    raw = {
        "apiVersion": "tekton.dev/v1",
        "kind": "PipelineRun",
        "metadata": {"name": name, "namespace": ns, "creationTimestamp": created},
        "spec": {},
        "status": status_dict,
    }
    if managed_fields:
        raw["metadata"]["managedFields"] = [{"manager": "oc", "operation": "Update"}]
    return raw


def raw_taskrun(
    name,
    ns="test-rhtap-1-tenant",
    status="True",
    reason="Succeeded",
    created="2026-08-13T10:00:06Z",
    started="2026-08-13T10:00:07Z",
    completed="2026-08-13T10:00:20Z",
    pod="pod-1",
    steps=(),
    msg=None,
):
    status_dict = {
        "startTime": started,
        "completionTime": completed,
        "podName": pod,
        "conditions": [{"type": "Succeeded", "status": status, "reason": reason, "message": msg}],
    }
    if steps:
        status_dict["steps"] = [
            {
                "name": sname,
                "container": f"step-{sname}",
                "terminated": {
                    "startedAt": sstart,
                    "finishedAt": sfinish,
                    "reason": "Completed",
                    "exitCode": 0,
                },
            }
            for sname, sstart, sfinish in steps
        ]
    return {
        "apiVersion": "tekton.dev/v1",
        "kind": "TaskRun",
        "metadata": {
            "name": name,
            "namespace": ns,
            "creationTimestamp": created,
            "labels": {
                "tekton.dev/pipeline": "load-test",
                "tekton.dev/pipelineTask": name.split("-")[-1],
            },
        },
        "spec": {},
        "status": status_dict,
    }


def raw_pod(
    name, ns="test-rhtap-1-tenant", phase="Succeeded", node="worker-1", conds=(), containers=()
):
    status_dict = {"phase": phase}
    if conds:
        status_dict["conditions"] = [
            {"type": ctype, "status": cstatus, "reason": creason, "lastTransitionTime": ctime}
            for ctype, cstatus, creason, ctime in conds
        ]
    if containers:
        status_dict["containerStatuses"] = [
            {
                "name": cname,
                "state": {
                    "terminated": {
                        "reason": creason,
                        "startedAt": cstart,
                        "finishedAt": cfinish,
                        "exitCode": cexit,
                    }
                },
            }
            for cname, creason, cstart, cfinish, cexit in containers
        ]
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": ns},
        "spec": {"nodeName": node},
        "status": status_dict,
    }


class StubCluster:
    """Minimal Cluster stand-in for the collect helpers."""

    def __init__(self, objects=None, logs=None):
        self.objects = objects or {}
        self.logs = logs or {}

    def get_object(self, kind, namespace, name):
        return self.objects.get((kind, namespace, name))

    def get_logs(self, namespace, pod, container):
        return self.logs.get((pod, container))


class FakeDynClient:
    """Stand-in for a kubernetes DynamicClient recording api_version attempts."""

    def __init__(self, served_versions, get_404_for=(), fail_discovery_for=()):
        self.served_versions = set(served_versions)
        self.get_404_for = set(get_404_for)
        self.fail_discovery_for = set(fail_discovery_for)
        self.calls = []

    class _Resources:
        def __init__(self, owner):
            self.owner = owner

        def get(self, api_version="", kind=""):
            self.owner.calls.append(("discover", api_version, kind))
            if api_version in self.owner.fail_discovery_for or (
                api_version not in self.owner.served_versions
            ):
                raise RuntimeError(f"version not served: {api_version}")
            return {"api_version": api_version}

    @property
    def resources(self):
        return self._Resources(self)

    def get(self, resource, namespace="", name=""):
        from kubernetes.client.rest import ApiException

        api_version = resource["api_version"]
        self.calls.append(("get", api_version, namespace, name))
        if api_version in self.get_404_for:
            raise ApiException(status=404)
        return {"metadata": {"name": name}, "ok": True}


def parse_cli(argv):
    """Parse a real CLI argv (argparse wiring is part of the contract)."""
    return plrtool.build_arg_parser().parse_args(argv)


# ---------------------------------------------------------------------------
# Time / stats utilities
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# KubeArchive config (kubectl-ka.conf)
# ---------------------------------------------------------------------------


SAMPLE_KA_CONF = {
    "kflux-ocp-p01": {
        "server_url": "https://api.kflux-ocp-p01.7ayg.p1.openshiftapps.com:6443",
        "host": "https://kubearchive-api-server-product-kubearchive.apps.kflux-ocp-p01.7ayg.p1.openshiftapps.com",
    },
    "stone-prd-rh01": {
        "server_url": "https://api.stone-prd-rh01.pg1f.p1.openshiftapps.com:6443",
        "host": "https://kubearchive-api-server-product-kubearchive.apps.stone-prd-rh01.pg1f.p1.openshiftapps.com",
    },
}


def test_ka_host_for_cluster():
    assert plrtool.ka_host_for_cluster(SAMPLE_KA_CONF, "kflux-ocp-p01").startswith(
        "https://kubearchive-api-server"
    )
    assert plrtool.ka_host_for_cluster(SAMPLE_KA_CONF, "unknown-cluster") is None
    assert plrtool.ka_host_for_cluster(SAMPLE_KA_CONF, None) is None
    # a cluster entry that exists but has no host
    assert plrtool.ka_host_for_cluster({"c": {"server_url": "https://x"}}, "c", "https://x") is None


def test_ka_host_matches_by_server_url():
    # kubeconfig cluster name is OCP-normalized (dots->dashes, +port); the conf
    # key is the raw dotted hostname -> only the server_url can match them.
    clusters = {
        "c111-e.us-east.containers.cloud.ibm.com": {
            "server_url": "https://c111-e.us-east.containers.cloud.ibm.com:32325",
            "host": "https://kubearchive.example",
        }
    }
    normalized_name = "c111-e-us-east-containers-cloud-ibm-com:32325"
    server = "https://c111-e.us-east.containers.cloud.ibm.com:32325"
    assert (
        plrtool.ka_host_for_cluster(clusters, normalized_name, server)
        == "https://kubearchive.example"
    )


def test_get_object_tries_all_api_versions():
    # api_version probing order: v1 first, v1beta1 as fallback.
    ka = FakeDynClient(
        served_versions={"tekton.dev/v1", "tekton.dev/v1beta1"},
        get_404_for={"tekton.dev/v1"},
    )
    obj = plrtool.Cluster._get_from(ka, "pipelinerun", "ns-1", "plr-1")
    assert obj["metadata"]["name"] == "plr-1"
    versions = [call[1] for call in ka.calls if call[0] == "discover"]
    assert versions == ["tekton.dev/v1", "tekton.dev/v1beta1"]


def test_get_from_falls_through_to_v1beta1_only():
    # v1 not in discovery at all -> falls through to v1beta1.
    ka = FakeDynClient(served_versions={"tekton.dev/v1beta1"})
    obj = plrtool.Cluster._get_from(ka, "taskrun", "ns-1", "tr-1")
    assert obj["metadata"]["name"] == "tr-1"
    versions = [call[1] for call in ka.calls if call[0] == "discover"]
    assert versions == ["tekton.dev/v1", "tekton.dev/v1beta1"]
    assert "get" in [call[0] for call in ka.calls]


def test_load_ka_conf(tmp_path):
    path = tmp_path / "ka.conf"
    path.write_text(
        yaml.safe_dump({"clusters": {"k1": {"host": "https://ka.example"}}}), encoding="utf-8"
    )
    assert plrtool.load_ka_conf(str(path)) == {"k1": {"host": "https://ka.example"}}
    bad = tmp_path / "bad.conf"
    bad.write_text(yaml.safe_dump({"clusters": []}), encoding="utf-8")
    try:
        plrtool.load_ka_conf(str(bad))
        raise AssertionError("expected ClusterError")
    except plrtool.ClusterError:
        pass


def test_ka_conf_host_resolution(monkeypatch, tmp_path):
    kc = tmp_path / "kubeconfig"
    kc.write_text(
        "current-context: my-ctx\n"
        "contexts:\n"
        "- name: my-ctx\n"
        "  context:\n"
        "    cluster: kflux-ocp-p01\n"
        "clusters:\n"
        "- name: kflux-ocp-p01\n"
        "  cluster:\n"
        "    server: https://api.example:6443\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(plrtool.cluster, "_kubeconfig_paths", lambda: [str(kc)])
    conf = tmp_path / "ka.conf"
    conf.write_text(yaml.safe_dump({"clusters": SAMPLE_KA_CONF}), encoding="utf-8")
    assert plrtool.cluster._ka_conf_host(str(conf)) == SAMPLE_KA_CONF["kflux-ocp-p01"]["host"]
    # resolution for a cluster not present in the conf -> None
    kc2 = tmp_path / "kubeconfig2"
    kc2.write_text(kc.read_text().replace("kflux-ocp-p01", "other-cluster"), encoding="utf-8")
    monkeypatch.setattr(plrtool.cluster, "_kubeconfig_paths", lambda: [str(kc2)])
    assert plrtool.cluster._ka_conf_host(str(conf)) is None
    # missing conf file -> None
    assert plrtool.cluster._ka_conf_host(str(tmp_path / "absent.conf")) is None


def test_ka_client_does_not_get_host_re_stamped_by_refresh_hook(monkeypatch):
    # Regression: load_kube_config installs refresh_api_key_hook on the main
    # config.  A copy of that config inherits the hook, and the hook re-applies
    # the live cluster's host on every request -> the KubeArchive "fallback"
    # silently queried the live cluster and 404'd.  The KA client must keep
    # the KubeArchive host even after the auth hook fires.
    from kubernetes import client as k8s_client

    ka_host = "https://kubearchive.example"
    main_cfg = k8s_client.Configuration()
    main_cfg.host = "https://live-cluster.example:6443"
    main_cfg.api_key["BearerToken"] = "Bearer test-token"

    def evil_refresh(conf):
        # exactly what KubeConfigLoader._set_config does on every request
        conf.host = "https://live-cluster.example:6443"
        conf.api_key["BearerToken"] = "Bearer test-token"

    main_cfg.refresh_api_key_hook = evil_refresh
    # avoid eager discovery network calls in the DynamicClient constructor
    from kubernetes import dynamic as dynamic_module

    class NoDiscoverDynamicClient(dynamic_module.client.DynamicClient):
        def __init__(self, client, cache_file=None, discoverer=None):
            self.client = client
            self.configuration = client.configuration

    monkeypatch.setattr(dynamic_module, "DynamicClient", NoDiscoverDynamicClient)
    client = plrtool.cluster._build_ka_client_from_host(main_cfg, ka_host)
    cfg = client.client.configuration
    assert cfg.host == ka_host
    # trigger the auth path (get_api_key_with_prefix runs refresh_api_key_hook)
    assert cfg.get_api_key_with_prefix("BearerToken") == "Bearer test-token"
    # host must NOT have been re-stamped to the live cluster
    assert cfg.host == ka_host


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


# ---------------------------------------------------------------------------
# Parsers + record subsetting
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# CacheStore: JSON dump + legacy YAML read
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# collect helpers with a stub cluster
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# timing analysis (cache only)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# errors analysis (cache only)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# CLI wiring
# ---------------------------------------------------------------------------


def test_cli_subcommand_help_and_defaults():
    args = parse_cli(["download", "--namespace", "n", "--plr", "p"])
    assert args.subcommand == "download"
    assert args.concurrency == plrtool.DEFAULT_CONCURRENCY
    assert args.cache == "collected-data"

    wait_args = parse_cli(["wait", "--namespace", "n", "--plr", "p"])
    assert wait_args.timeout == "100m"

    timing_args = parse_cli(["timing", "--gantt-chart", "g.png", "--summary", "s.json"])
    assert timing_args.gantt_chart == "g.png"
    assert timing_args.summary == "s.json"


def test_cli_download_with_timing_builds_options():
    args = parse_cli(
        [
            "download",
            "--namespace",
            "n",
            "--plr",
            "p",
            "--with-timing",
            "--gantt-chart",
            "g.png",
        ]
    )
    options = DownloadOptions(
        cache_dir=Path(args.cache),
        concurrency=args.concurrency,
        with_timing=(
            TimingOptions(gantt_chart=args.gantt_chart, summary=args.summary)
            if args.with_timing
            else None
        ),
    )
    assert options.with_timing.gantt_chart == "g.png"
    assert options.with_timing.summary is None
