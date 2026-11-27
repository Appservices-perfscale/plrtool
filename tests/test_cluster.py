"""Unit tests for cluster access: KubeArchive config resolution and API-version probing."""

import json
import types

import yaml
from helpers import FakeDynClient

import plrtool
from plrtool.cluster import Cluster, _build_ka_client_from_host, _ka_conf_host


class FakeKaApiClient:
    """Plain-ApiClient stand-in for _KubeArchiveClient (no network)."""

    def __init__(self, fail_with=(), pages=None, raw_pages=None):
        self.configuration = types.SimpleNamespace(host="https://ka.example")
        self.fail_with = set(fail_with)
        self.pages = list(pages or [])
        self.raw_pages = list(raw_pages or [])
        self.calls = []

    def call_api(self, path, method, **kwargs):
        self.calls.append((path, method, kwargs))
        if path in self.fail_with:
            from kubernetes.client.rest import ApiException

            raise ApiException(status=404)
        if self.raw_pages:
            return types.SimpleNamespace(data=self.raw_pages.pop(0))
        if self.pages:
            payload = self.pages.pop(0)
        else:
            payload = {"metadata": {"name": "x"}, "ok": True}
        return types.SimpleNamespace(data=json.dumps(payload).encode())


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
    obj = Cluster._get_from(ka, "pipelinerun", "ns-1", "plr-1")
    assert obj["metadata"]["name"] == "plr-1"
    versions = [call[1] for call in ka.calls if call[0] == "discover"]
    assert versions == ["tekton.dev/v1", "tekton.dev/v1beta1"]


def test_get_from_falls_through_to_v1beta1_only():
    # v1 not in discovery at all -> falls through to v1beta1.
    ka = FakeDynClient(served_versions={"tekton.dev/v1beta1"})
    obj = Cluster._get_from(ka, "taskrun", "ns-1", "tr-1")
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
    assert _ka_conf_host(str(conf)) == SAMPLE_KA_CONF["kflux-ocp-p01"]["host"]
    # resolution for a cluster not present in the conf -> None
    kc2 = tmp_path / "kubeconfig2"
    kc2.write_text(kc.read_text().replace("kflux-ocp-p01", "other-cluster"), encoding="utf-8")
    monkeypatch.setattr(plrtool.cluster, "_kubeconfig_paths", lambda: [str(kc2)])
    assert _ka_conf_host(str(conf)) is None
    # missing conf file -> None
    assert _ka_conf_host(str(tmp_path / "absent.conf")) is None


def test_resolve_ka_client_warns_when_unconfigured(monkeypatch, caplog):
    # No KA context, no ka.conf entry, no auto-detected context -> None,
    # but a warning with the 'oc ka' fix hint must reach the user.
    import logging

    cluster = object.__new__(Cluster)
    cluster.ka_context = None
    cluster.ka_conf = None
    monkeypatch.setattr(plrtool.cluster, "_ka_conf_host", lambda ka_conf=None: None)
    monkeypatch.setattr(plrtool.cluster, "_find_ka_context", lambda: None)
    with caplog.at_level(logging.WARNING, logger="plrtool"):
        assert cluster._resolve_ka_client() is None
    assert "oc ka" in caplog.text


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
    client = _build_ka_client_from_host(main_cfg, ka_host)
    cfg = client.client.configuration
    assert cfg.host == ka_host
    # trigger the auth path (get_api_key_with_prefix runs refresh_api_key_hook)
    assert cfg.get_api_key_with_prefix("BearerToken") == "Bearer test-token"
    # host must NOT have been re-stamped to the live cluster
    assert cfg.host == ka_host


def test_ka_client_builds_grouped_and_core_urls():
    from plrtool.cluster import _KubeArchiveClient

    api = FakeKaApiClient()
    client = _KubeArchiveClient(api)
    client.get(
        client.resources.get(api_version="tekton.dev/v1", kind="PipelineRun"),
        namespace="ns-1",
        name="plr-1",
    )
    client.get(
        client.resources.get(api_version="tekton.dev/v1beta1", kind="TaskRun"),
        namespace="ns-1",
        name="tr-1",
    )
    client.get(client.resources.get(api_version="v1", kind="Pod"), namespace="ns-1", name="pod-1")
    paths = [path for path, _, _ in api.calls]
    assert paths == [
        "/apis/tekton.dev/v1/namespaces/ns-1/pipelineruns/plr-1",
        "/apis/tekton.dev/v1beta1/namespaces/ns-1/taskruns/tr-1",
        "/api/v1/namespaces/ns-1/pods/pod-1",
    ]
    assert api.calls[0][2]["auth_settings"] == ["BearerToken"]


def test_ka_client_logs_url_and_decoding():
    from plrtool.cluster import _KubeArchiveClient

    api = FakeKaApiClient()
    client = _KubeArchiveClient(api)
    text = client.get_logs("ns-1", "pod-1", "step-build")
    path, method, kwargs = api.calls[0]
    assert path == "/api/v1/namespaces/ns-1/pods/pod-1/log?container=step-build"
    assert method == "GET"
    assert kwargs["header_params"] == {"Accept": "text/plain"}
    assert kwargs["auth_settings"] == ["BearerToken"]
    assert text == json.dumps({"metadata": {"name": "x"}, "ok": True})


class FakeNewStyleKaApiClient:
    """kubernetes>=37-style ApiClient: slim call_api, no legacy kwargs."""

    def __init__(self, payload=None, status=200):
        self.configuration = types.SimpleNamespace(
            host="https://ka.example",
            auth_settings=lambda: {
                "BearerToken": {
                    "type": "api_key",
                    "in": "header",
                    "key": "authorization",
                    "value": "Bearer token",
                }
            },
        )
        self.calls = []
        self.payload = (
            payload
            if payload is not None
            else json.dumps({"metadata": {"name": "x"}, "ok": True}).encode()
        )
        self.status = status

    def call_api(
        self, method, url, header_params=None, body=None, post_params=None, _request_timeout=None
    ):
        self.calls.append((method, url, header_params))
        payload = (
            self.payload
            if url.endswith("/log") or "?container=" in url
            else json.dumps({"metadata": {"name": "x"}, "ok": True}).encode()
        )
        return types.SimpleNamespace(
            status=self.status,
            reason="reason",
            data=payload,
            read=lambda: payload,
        )


def test_ka_client_works_with_new_style_call_api():
    """kubernetes>=37 dropped auth_settings/_preload_content from call_api."""
    from plrtool.cluster import _KubeArchiveClient

    api = FakeNewStyleKaApiClient(payload=b"line1\nline2\n")
    client = _KubeArchiveClient(api)

    # get_logs through the new-style call_api fallback
    text = client.get_logs("ns-1", "pod-1", "step-clone")
    assert text == "line1\nline2\n"
    method, url, headers = api.calls[0]
    assert method == "GET"
    assert url == "https://ka.example/api/v1/namespaces/ns-1/pods/pod-1/log?container=step-clone"
    assert headers == {"Accept": "text/plain", "authorization": "Bearer token"}

    # object fetch through the same fallback
    obj = client.get(
        client.resources.get(api_version="tekton.dev/v1", kind="PipelineRun"),
        namespace="ns-1",
        name="plr-1",
    )
    assert obj["ok"] is True
    assert api.calls[1][1] == (
        "https://ka.example/apis/tekton.dev/v1/namespaces/ns-1/pipelineruns/plr-1"
    )


def test_ka_client_new_style_raises_api_exception_on_404():
    from kubernetes.client.rest import ApiException

    from plrtool.cluster import _KubeArchiveClient

    api = FakeNewStyleKaApiClient(status=404)
    client = _KubeArchiveClient(api)
    try:
        client.get_logs("ns-1", "pod-1", "step-clone")
        raise AssertionError("expected ApiException")
    except ApiException as exc:
        assert exc.status == 404


def test_get_logs_prefers_live_cluster():
    from plrtool.cluster import _KubeArchiveClient

    cluster = object.__new__(Cluster)
    api = FakeKaApiClient()
    cluster.ka_dyn = _KubeArchiveClient(api)

    class OkCore:
        def read_namespaced_pod_log(self, name, namespace, container, _preload_content=True):
            # real CoreV1Api signature: name (pod) first, namespace second.
            return f"live-log:{namespace}/{name}/{container}"

    cluster.core = OkCore()
    assert cluster.get_logs("ns-1", "pod-1", "c-1") == "live-log:ns-1/pod-1/c-1"
    assert api.calls == []  # live cluster served the logs; KA never consulted


def test_get_logs_decodes_live_response_bytes():
    """Live fetch returns a response object; body bytes must become real text."""
    cluster = object.__new__(Cluster)
    cluster.ka_dyn = None

    class BytesCore:
        def read_namespaced_pod_log(self, name, namespace, container, _preload_content=True):
            assert _preload_content is False  # deserialization must be bypassed
            return types.SimpleNamespace(data=b"line1\nline2\n")

    cluster.core = BytesCore()
    assert cluster.get_logs("n", "p", "c") == "line1\nline2\n"


def test_get_logs_live_payload_with_lazy_read():
    """Response whose data is only filled by read() (kubernetes RESTResponse)."""
    cluster = object.__new__(Cluster)
    cluster.ka_dyn = None

    class LazyCore:
        def read_namespaced_pod_log(self, name, namespace, container, _preload_content=True):
            resp = types.SimpleNamespace(status=200, data=None)

            def read():
                resp.data = b"lazy\nlog\n"
                return resp.data

            resp.read = read
            return resp

    cluster.core = LazyCore()
    assert cluster.get_logs("n", "p", "c") == "lazy\nlog\n"


def test_get_logs_falls_back_to_kubearchive():
    from kubernetes.client.rest import ApiException

    from plrtool.cluster import _KubeArchiveClient

    cluster = object.__new__(Cluster)
    api = FakeKaApiClient()
    cluster.ka_dyn = _KubeArchiveClient(api)

    class FailCore:
        def read_namespaced_pod_log(self, name, namespace, container, _preload_content=True):
            raise ApiException(status=404)

    cluster.core = FailCore()
    assert cluster.get_logs("n", "p", "c") == json.dumps({"metadata": {"name": "x"}, "ok": True})
    assert api.calls[0][0] == "/api/v1/namespaces/n/pods/p/log?container=c"


def test_get_logs_returns_none_without_kubearchive():
    from kubernetes.client.rest import ApiException

    cluster = object.__new__(Cluster)
    cluster.ka_dyn = None

    class FailCore:
        def read_namespaced_pod_log(self, name, namespace, container, _preload_content=True):
            raise ApiException(status=404)

    cluster.core = FailCore()
    assert cluster.get_logs("n", "p", "c") is None


def test_get_from_ka_client_falls_through_api_versions_on_404():
    from plrtool.cluster import _KubeArchiveClient

    api = FakeKaApiClient(fail_with=("/apis/tekton.dev/v1/namespaces/ns-1/pipelineruns/plr-1",))
    obj = Cluster._get_from(_KubeArchiveClient(api), "pipelinerun", "ns-1", "plr-1")
    assert obj["ok"] is True
    paths = [path for path, _, _ in api.calls]
    assert paths == [
        "/apis/tekton.dev/v1/namespaces/ns-1/pipelineruns/plr-1",
        "/apis/tekton.dev/v1beta1/namespaces/ns-1/pipelineruns/plr-1",
    ]


def test_ka_client_list_builds_url_with_filters():
    from plrtool.cluster import _KubeArchiveClient

    api = FakeKaApiClient(pages=[{"items": [], "metadata": {}}])
    client = _KubeArchiveClient(api)
    client.list(
        client.resources.get(api_version="tekton.dev/v1", kind="PipelineRun"),
        namespace="ns-1",
        limit=500,
        creationTimestampAfter="2026-08-01T00:00:00Z",
    )
    path = api.calls[0][0]
    assert path.startswith("/apis/tekton.dev/v1/namespaces/ns-1/pipelineruns?")
    assert "limit=500" in path
    assert "creationTimestampAfter=2026-08-01T00%3A00%3A00Z" in path


def test_list_from_follows_continue_tokens(caplog):
    import logging

    from plrtool.cluster import _KubeArchiveClient

    api = FakeKaApiClient(
        pages=[
            {"items": [{"metadata": {"name": "plr-1"}}], "metadata": {"continue": "tok-1"}},
            {"items": [{"metadata": {"name": "plr-2"}}], "metadata": {}},
        ]
    )
    with caplog.at_level(logging.DEBUG, logger="plrtool"):
        items = Cluster._list_from(_KubeArchiveClient(api), "pipelinerun", "ns-1")
    assert [item["metadata"]["name"] for item in items] == ["plr-1", "plr-2"]
    paths = [path for path, _, _ in api.calls]
    assert paths[0].endswith("?limit=100")
    assert "continue=tok-1" in paths[1]
    assert "limit=100" in paths[1]
    # one DEBUG line per page, with running totals
    page_lines = [line for line in caplog.text.splitlines() if "page 1 returned" in line]
    assert len(page_lines) == 1
    assert "1 item(s), 1 total, more=True" in page_lines[0]
    assert "page 2 returned 1 item(s), 2 total, more=False" in caplog.text


def test_ka_client_list_wraps_truncated_json():
    """Truncated KubeArchive body (HTTP 200, mid-stream DB error) -> ClusterError."""
    from plrtool.cluster import _KubeArchiveClient

    api = FakeKaApiClient(raw_pages=[b'{"kind": "List", "items": [{"a": 1}'])
    client = _KubeArchiveClient(api)
    try:
        client.list(client.resources.get(api_version="v1", kind="Pod"), namespace="ns-1")
        raise AssertionError("expected ClusterError")
    except plrtool.ClusterError as exc:
        assert "truncated" in str(exc)
        assert "35 bytes" in str(exc)


def test_list_pipelinerun_names_unions_and_filters():
    import datetime as dt

    from plrtool.cluster import _KubeArchiveClient

    cluster = object.__new__(Cluster)
    cluster.dyn = FakeDynClient(
        served_versions={"tekton.dev/v1"},
        list_pages=[
            {
                "items": [
                    {
                        "metadata": {
                            "name": "live-old",
                            "creationTimestamp": "2026-07-31T00:00:00Z",
                        }
                    },
                    {
                        "metadata": {
                            "name": "live-new",
                            "creationTimestamp": "2026-08-02T00:00:00Z",
                        }
                    },
                ],
                "metadata": {},
            }
        ],
    )
    api = FakeKaApiClient(
        pages=[
            {
                "items": [
                    {
                        "metadata": {
                            "name": "live-new",
                            "creationTimestamp": "2026-08-02T00:00:00Z",
                        }
                    },
                    {
                        "metadata": {
                            "name": "ka-only",
                            "creationTimestamp": "2026-08-03T00:00:00Z",
                        }
                    },
                ],
                "metadata": {"continue": "tok"},
            },
            {
                "items": [
                    {
                        "metadata": {
                            "name": "ka-page2",
                            "creationTimestamp": "2026-08-04T00:00:00Z",
                        }
                    }
                ],
                "metadata": {},
            },
        ]
    )
    cluster.ka_dyn = _KubeArchiveClient(api)
    since = dt.datetime(2026, 8, 1, tzinfo=dt.UTC)
    until = dt.datetime(2026, 8, 5, tzinfo=dt.UTC)
    # strict bounds: live-old (== since boundary excluded too if equal)
    assert cluster.list_pipelinerun_names("ns-1", since, until) == [
        "live-new",
        "ka-only",
        "ka-page2",
    ]
    # KubeArchive got the server-side time filters, the live cluster did not.
    ka_paths = [path for path, _, _ in api.calls]
    assert "creationTimestampAfter=2026-08-01T00%3A00%3A00Z" in ka_paths[0]
    dyn_list_calls = [call for call in cluster.dyn.calls if call[0] == "list"]
    assert dyn_list_calls[0][3] == {"limit": 100}


def test_list_pipelinerun_names_raises_when_all_backends_fail():
    cluster = object.__new__(Cluster)
    cluster.dyn = FakeDynClient(served_versions=set())
    cluster.ka_dyn = None
    try:
        cluster.list_pipelinerun_names("ns-1")
        raise AssertionError("expected ClusterError")
    except plrtool.ClusterError:
        pass


def test_delete_pipelinerun_requests_background_propagation():
    dyn = FakeDynClient(served_versions={"tekton.dev/v1"})
    cluster = object.__new__(Cluster)
    cluster.dyn = dyn
    assert cluster.delete_pipelinerun("ns-1", "plr-1") is True
    assert [call[0] for call in dyn.calls] == ["discover", "delete"]
    assert dyn.calls[1][1:4] == ("tekton.dev/v1", "ns-1", "plr-1")
    assert dyn.calls[1][4]["propagation_policy"] == "Background"


def test_delete_pipelinerun_404_is_success():
    # PLR already gone -> delete is a no-op success, not an error.
    dyn = FakeDynClient(served_versions={"tekton.dev/v1"}, delete_404_for={"tekton.dev/v1"})
    cluster = object.__new__(Cluster)
    cluster.dyn = dyn
    assert cluster.delete_pipelinerun("ns-1", "plr-1") is True


def test_delete_pipelinerun_falls_through_api_versions():
    # v1 delete fails with 500 -> v1beta1 is tried next.
    dyn = FakeDynClient(
        served_versions={"tekton.dev/v1", "tekton.dev/v1beta1"},
        delete_error_for={"tekton.dev/v1"},
    )
    cluster = object.__new__(Cluster)
    cluster.dyn = dyn
    assert cluster.delete_pipelinerun("ns-1", "plr-1") is True
    versions = [call[1] for call in dyn.calls if call[0] == "delete"]
    assert versions == ["tekton.dev/v1", "tekton.dev/v1beta1"]


def test_delete_pipelinerun_all_fail_returns_false():
    dyn = FakeDynClient(
        served_versions={"tekton.dev/v1", "tekton.dev/v1beta1"},
        delete_error_for={"tekton.dev/v1", "tekton.dev/v1beta1"},
    )
    cluster = object.__new__(Cluster)
    cluster.dyn = dyn
    assert cluster.delete_pipelinerun("ns-1", "plr-1") is False


def test_live_get_returns_none_on_404():
    dyn = FakeDynClient(
        served_versions={"tekton.dev/v1", "tekton.dev/v1beta1"},
        get_404_for={"tekton.dev/v1", "tekton.dev/v1beta1"},
    )
    cluster = object.__new__(Cluster)
    cluster.dyn = dyn
    assert cluster._live_get("pipelinerun", "ns-1", "plr-1") is None


def test_live_get_returns_object_when_present():
    dyn = FakeDynClient(served_versions={"tekton.dev/v1"})
    cluster = object.__new__(Cluster)
    cluster.dyn = dyn
    obj = cluster._live_get("pipelinerun", "ns-1", "plr-1")
    assert obj is not None
    assert obj["metadata"]["name"] == "plr-1"


def test_wait_deleted_true_when_already_gone():
    dyn = FakeDynClient(
        served_versions={"tekton.dev/v1", "tekton.dev/v1beta1"},
        get_404_for={"tekton.dev/v1", "tekton.dev/v1beta1"},
    )
    cluster = object.__new__(Cluster)
    cluster.dyn = dyn
    assert cluster.wait_deleted("ns-1", "plr-1", timeout=60) is True


def test_wait_deleted_times_out_when_still_present():
    dyn = FakeDynClient(served_versions={"tekton.dev/v1", "tekton.dev/v1beta1"})
    cluster = object.__new__(Cluster)
    cluster.dyn = dyn
    assert cluster.wait_deleted("ns-1", "plr-1", timeout=0.01) is False


def test_get_object_404_debug_mentions_kubearchive_fallback(caplog):
    # A live-cluster 404 must say the search continues in KubeArchive,
    # so users do not read it as "object does not exist".
    import logging

    from plrtool.cluster import _KubeArchiveClient

    dyn = FakeDynClient(
        served_versions={"tekton.dev/v1", "tekton.dev/v1beta1"},
        get_404_for={"tekton.dev/v1", "tekton.dev/v1beta1"},
    )
    cluster = object.__new__(Cluster)
    cluster.dyn = dyn
    cluster.ka_dyn = _KubeArchiveClient(FakeKaApiClient())
    with caplog.at_level(logging.DEBUG, logger="plrtool"):
        assert cluster.get_object("pipelinerun", "ns-1", "plr-1") is not None
    cluster_line = [
        line for line in caplog.text.splitlines() if "cluster: pipelinerun ns-1/plr-1" in line
    ][0]
    assert "not found" in cluster_line
    assert "will try KubeArchive now" in cluster_line
