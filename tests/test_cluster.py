"""Unit tests for cluster access: KubeArchive config resolution and API-version probing."""

import json
import types

import yaml
from helpers import FakeDynClient

import plrtool
from plrtool.cluster import Cluster, _build_ka_client_from_host, _ka_conf_host


class FakeKaApiClient:
    """Plain-ApiClient stand-in for _KubeArchiveClient (no network)."""

    def __init__(self, fail_with=()):
        self.configuration = types.SimpleNamespace(host="https://ka.example")
        self.fail_with = set(fail_with)
        self.calls = []

    def call_api(self, path, method, **kwargs):
        self.calls.append((path, method, kwargs))
        if path in self.fail_with:
            from kubernetes.client.rest import ApiException

            raise ApiException(status=404)
        return types.SimpleNamespace(
            data=json.dumps({"metadata": {"name": "x"}, "ok": True}).encode()
        )


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


def test_get_logs_prefers_live_cluster():
    from plrtool.cluster import _KubeArchiveClient

    cluster = object.__new__(Cluster)
    api = FakeKaApiClient()
    cluster.ka_dyn = _KubeArchiveClient(api)

    class OkCore:
        def read_namespaced_pod_log(self, namespace, pod, container):
            return "live-log"

    cluster.core = OkCore()
    assert cluster.get_logs("n", "p", "c") == "live-log"
    assert api.calls == []  # live cluster served the logs; KA never consulted


def test_get_logs_falls_back_to_kubearchive():
    from kubernetes.client.rest import ApiException

    from plrtool.cluster import _KubeArchiveClient

    cluster = object.__new__(Cluster)
    api = FakeKaApiClient()
    cluster.ka_dyn = _KubeArchiveClient(api)

    class FailCore:
        def read_namespaced_pod_log(self, namespace, pod, container):
            raise ApiException(status=404)

    cluster.core = FailCore()
    assert cluster.get_logs("n", "p", "c") == json.dumps({"metadata": {"name": "x"}, "ok": True})
    assert api.calls[0][0] == "/api/v1/namespaces/n/pods/p/log?container=c"


def test_get_logs_returns_none_without_kubearchive():
    from kubernetes.client.rest import ApiException

    cluster = object.__new__(Cluster)
    cluster.ka_dyn = None

    class FailCore:
        def read_namespaced_pod_log(self, namespace, pod, container):
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
