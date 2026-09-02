"""Cluster: direct Kubernetes API access (no ``oc`` subprocesses).

Kinds are fetched from the live cluster API first; when that fails the client
falls back to KubeArchive.  KubeArchive is resolved in order of:
1. an explicit kubeconfig context (--ka-context)
2. a kubectl-ka.conf endpoint for the current cluster (--ka-conf)
3. an auto-detected kubeconfig context whose name/server host it (kubearchive)
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import yaml

from .constants import POLL_INTERVAL
from .exceptions import ClusterError

logger = logging.getLogger("plrtool")

__all__ = [
    "DEFAULT_KA_CONF",
    "DEFAULT_KA_CONF_ENV",
    "KIND_API",
    "Cluster",
    "ka_host_for_cluster",
    "load_ka_conf",
]
# ---------------------------------------------------------------------------
# 5. Cluster: direct API access loaded from kubeconfig
# ---------------------------------------------------------------------------

# api_version candidates are tried in order: the live cluster and KubeArchive
# both serve tekton as v1 (KubeArchive's aggregated tekton.dev/v1 returns the
# archived object; v1beta1 404s for it), so v1 is tried first with v1beta1 as
# a fallback for older servers.  The first api_version that resolves and
# returns a hit wins.
KIND_API = {
    "pipelinerun": (("tekton.dev/v1", "tekton.dev/v1beta1"), "PipelineRun"),
    "taskrun": (("tekton.dev/v1", "tekton.dev/v1beta1"), "TaskRun"),
    "pod": (("v1",), "Pod"),
}


def _kubeconfig_paths() -> list[str]:
    """Return the kubeconfig file paths to consider (KUBECONFIG first)."""
    env = os.environ.get("KUBECONFIG")
    if env:
        return [path for path in env.split(os.pathsep) if path]
    return [str(Path.home() / ".kube" / "config")]


def _current_cluster() -> tuple[str | None, str | None]:
    """Return (cluster_name, server_url) of the current kubeconfig context."""
    for path in _kubeconfig_paths():
        try:
            data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001 - tolerate unreadable kubeconfigs
            logger.debug("could not read kubeconfig %s: %s", path, exc)
            continue
        if not isinstance(data, dict):
            continue
        clusters = {
            (cluster.get("name") or ""): (cluster.get("cluster") or {})
            for cluster in data.get("clusters") or []
        }
        current = data.get("current-context")
        for context in data.get("contexts") or []:
            if context.get("name") == current:
                cluster_name = (context.get("context") or {}).get("cluster")
                server = (clusters.get(cluster_name) or {}).get("server")
                return cluster_name, server
    return None, None


def _current_cluster_name() -> str | None:
    """Return the cluster name of the current kubeconfig context, or None."""
    return _current_cluster()[0]


def _normalize_url(url: object) -> str | None:
    """Canonical form of a server URL for comparison (host:port, lowercased)."""
    if not url:
        return None
    text = str(url).strip()
    for scheme in ("https://", "http://"):
        text = text.removeprefix(scheme)
    text = text.rstrip("/").lower()
    return text or None


DEFAULT_KA_CONF = Path.home() / ".config" / "kubectl-ka.conf"
DEFAULT_KA_CONF_ENV = "KUBECTL_KA_CONFIG"


def load_ka_conf(path: str) -> dict:
    """Load a kubectl-ka.conf file -> {cluster_name: {server_url, host}}."""
    try:
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        raise ClusterError(f"cannot read KubeArchive config {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ClusterError(f"{path}: expected a mapping at top level")
    clusters = data.get("clusters")
    if not isinstance(clusters, dict):
        raise ClusterError(f"{path}: missing 'clusters' mapping")
    return clusters


def ka_host_for_cluster(
    clusters: dict,
    cluster_name: str | None = None,
    server_url: str | None = None,
) -> str | None:
    """KubeArchive API host for a cluster, or None.

    Matches by kubeconfig cluster name first (exact), then by API server URL
    (normalized).  Kubeconfig cluster names are often OCP-normalized
    (dots->dashes, with port) while kubectl-ka.conf keys use the raw server
    hostname, so server_url matching is the reliable fallback.
    """
    if cluster_name:
        entry = clusters.get(cluster_name)
        if isinstance(entry, dict):
            return entry.get("host")
    if server_url:
        want = _normalize_url(server_url)
        if want:
            for entry in clusters.values():
                if isinstance(entry, dict) and _normalize_url(entry.get("server_url")) == want:
                    return entry.get("host")
    return None


def _ka_conf_host(ka_conf: str | None = None) -> str | None:
    """Resolve the KubeArchive API host for the current cluster.

    Reads a kubectl-ka.conf (--ka-conf, $KUBECTL_KA_CONFIG, or
    ~/.config/kubectl-ka.conf) and looks up the cluster of the current
    kubeconfig context.  Returns None when no KA endpoint is configured.
    """
    path = ka_conf or os.environ.get(DEFAULT_KA_CONF_ENV) or str(DEFAULT_KA_CONF)
    if not Path(path).is_file():
        logger.debug("no KubeArchive config file: %s", path)
        return None
    clusters = load_ka_conf(path)
    cluster_name, server = _current_cluster()
    host = ka_host_for_cluster(clusters, cluster_name, server)
    if host is None:
        logger.warning(
            "no KubeArchive host for cluster %r (server %r) in %s",
            cluster_name,
            server,
            path,
        )
        return None
    logger.debug("KubeArchive host for cluster %r: %s", cluster_name, host)
    return host


def _find_ka_context() -> str | None:
    """Best-effort: find a kubeconfig context pointing at KubeArchive.

    Looks for a context whose name contains 'kubearchive' or whose cluster
    server URL does.  Returns None when none is found (fallback disabled).
    """
    for path in _kubeconfig_paths():
        try:
            data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001 - tolerate unreadable kubeconfigs
            logger.debug("could not read kubeconfig %s: %s", path, exc)
            continue
        if not isinstance(data, dict):
            continue
        clusters = {
            (cluster.get("name") or ""): (cluster.get("cluster") or {})
            for cluster in data.get("clusters") or []
        }
        for context in data.get("contexts") or []:
            context_meta = context.get("context") or {}
            cluster_name = context_meta.get("cluster")
            server = (clusters.get(cluster_name) or {}).get("server") or ""
            candidate = context.get("name") or ""
            if "kubearchive" in candidate.lower() or "kubearchive" in (server or "").lower():
                return candidate
    return None


def _build_api_client(context: str | None) -> Any:
    """Build a Kubernetes DynamicClient from a kubeconfig context (or current)."""
    from kubernetes import client as kubernetes_client
    from kubernetes import config as kubernetes_config
    from kubernetes.dynamic import DynamicClient

    configuration = kubernetes_client.Configuration()
    try:
        kubernetes_config.load_kube_config(client_configuration=configuration, context=context)
    except Exception as exc:
        raise ClusterError(f"cannot load kubeconfig (context={context!r}): {exc}") from exc
    return DynamicClient(kubernetes_client.ApiClient(configuration))


class _KubeArchiveResource:
    """Path token for one KubeArchive resource (DynamicClient Resource stand-in)."""

    __slots__ = ("group", "resource", "version")

    def __init__(self, group: str, version: str, resource: str):
        """Store the path parts of a KubeArchive resource request."""
        self.group = group
        self.version = version
        self.resource = resource


class _KubeArchiveResources:
    """`resources` attribute of _KubeArchiveClient (DynamicClient-like)."""

    def __init__(self, client: _KubeArchiveClient):
        """Bind the finder to its owning client."""
        self._client = client

    def get(self, api_version: str = "", kind: str = "") -> _KubeArchiveResource:
        """Resolve an api_version + kind to its KubeArchive path token.

        KubeArchive serves archived objects under the resource's own
        apiVersion (``/apis/<group>/<version>/...``, core under
        ``/api/<version>/...``), so group/version split the api_version and
        the path uses the plural lowercase resource name.  No network access
        happens here (the token only carries path parts).
        """
        if "/" in api_version:
            group, version = api_version.split("/", 1)
        else:
            group, version = "", api_version
        # Kinds handled by plrtool pluralize with a plain trailing "s";
        # anything else falls back to lowercase + "s" as a best effort.
        plural = {"PipelineRun": "pipelineruns", "TaskRun": "taskruns", "Pod": "pods"}.get(
            kind, f"{kind.lower()}s"
        )
        return _KubeArchiveResource(group, version, plural)


class _KubeArchiveClient:
    """DynamicClient-compatible adapter for the KubeArchive API.

    KubeArchive (Web API) serves archived objects at the object's own
    apiVersion path (``/apis/<group>/<version>/namespaces/<ns>/<plural>/<name>``,
    core under ``/api/<version>/...``) and exposes NO Kubernetes discovery
    endpoints (``/version``, ``/api``, ``/apis`` all 404).  A stock
    DynamicClient would die on construction because its eager discovery
    requests ``/version`` first.  This adapter exposes just the DynamicClient
    surface plrtool uses (``resources.get`` + ``get``) over a plain
    authenticated ``kubernetes.client.ApiClient``.
    """

    def __init__(self, api_client: Any):
        """Wrap a plain kubernetes ApiClient pointed at the KubeArchive host."""
        self.client = api_client
        self.configuration = api_client.configuration

    @property
    def resources(self) -> _KubeArchiveResources:
        """Finder for resource path tokens (mirrors DynamicClient.resources)."""
        return _KubeArchiveResources(self)

    def get(self, resource: _KubeArchiveResource, namespace: str = "", name: str = "") -> dict:
        """Fetch one archived object as a plain dict; 404 raises ApiException.

        The request goes through the ApiClient so the bearer token / client
        certs from the main cluster configuration are applied, the same way
        DynamicClient talks to a live cluster.
        """
        if resource.group:
            path = (
                f"/apis/{resource.group}/{resource.version}"
                f"/namespaces/{namespace}/{resource.resource}/{name}"
            )
        else:
            path = f"/api/{resource.version}/namespaces/{namespace}/{resource.resource}/{name}"
        response = self.client.call_api(
            path,
            "GET",
            header_params={"Accept": "application/json"},
            auth_settings=["BearerToken"],
            _preload_content=False,
            _return_http_data_only=True,
        )
        return json.loads(response.data.decode("utf-8"))


def _build_ka_client_from_context(context: str) -> Any:
    """Build a KubeArchive client from a kubeconfig context (no discovery).

    Raises ClusterError when the context cannot be loaded.
    """
    from kubernetes import client as kubernetes_client
    from kubernetes import config as kubernetes_config

    configuration = kubernetes_client.Configuration()
    try:
        kubernetes_config.load_kube_config(client_configuration=configuration, context=context)
    except Exception as exc:
        raise ClusterError(f"cannot load kubeconfig (context={context!r}): {exc}") from exc
    return _KubeArchiveClient(kubernetes_client.ApiClient(configuration))


def _build_ka_client_from_host(main_cfg: Any, host: str) -> Any:
    """Build a KubeArchive client for a KubeArchive API host.

    Returns a _KubeArchiveClient (plain ApiClient, no Kubernetes discovery -
    KubeArchive has none and 404s ``/version`` outright).  Reuses the
    authentication (bearer token / certs) from the main cluster configuration;
    KubeArchive authenticates via TokenReview against the cluster it fronts,
    so the same token works.

    A plain copy of the main configuration is NOT safe here: load_kube_config
    installs refresh_api_key_hook on it, and that hook re-applies the whole
    cluster context (host, TLS, token) onto the configuration, so the first
    request would re-stamp the live cluster's host over our KubeArchive host
    (silently turning the "fallback" into a second query against the live
    cluster).  We copy the auth/TLS fields we actually need instead.
    """
    from kubernetes import client as kubernetes_client

    ka_cfg = kubernetes_client.Configuration()
    ka_cfg.host = host if host.startswith(("http://", "https://")) else f"https://{host}"
    for attr in (
        "api_key",
        "api_key_prefix",
        "ssl_ca_cert",
        "cert_file",
        "key_file",
        "verify_ssl",
        "assert_hostname",
        "tls_server_name",
        "proxy",
        "no_proxy",
    ):
        if hasattr(main_cfg, attr):
            try:
                setattr(ka_cfg, attr, getattr(main_cfg, attr))
            except Exception as exc:  # noqa: BLE001 - tolerate odd auth setups
                logger.debug(
                    "could not copy auth config attr %s onto KubeArchive client: %s", attr, exc
                )
    return _KubeArchiveClient(kubernetes_client.ApiClient(ka_cfg))


def _to_plain_dict(obj: Any) -> dict:
    """Convert a Kubernetes client response object into a plain dict."""
    to_dict = getattr(obj, "to_dict", None)
    if callable(to_dict):
        return to_dict()
    if isinstance(obj, dict):
        return obj
    return dict(obj)


class Cluster:
    """Direct Kubernetes API access (no `oc` subprocesses).

    Kinds are fetched from the live cluster API first; when that fails the
    client falls back to KubeArchive.  KubeArchive is resolved in order of:
    1. an explicit kubeconfig context (--ka-context)
    2. a kubectl-ka.conf endpoint for the current cluster (--ka-conf)
    3. an auto-detected kubeconfig context whose name/server host it
       (kubearchive)
    """

    def __init__(self, ka_context: str | None = None, ka_conf: str | None = None):
        self.ka_context = ka_context
        self.ka_conf = ka_conf
        self.dyn = _build_api_client(None)
        self.ka_dyn = self._resolve_ka_client()
        from kubernetes import client as kubernetes_client

        self.core = kubernetes_client.CoreV1Api(self.dyn.client)

    def _resolve_ka_client(self) -> Any:
        """Build the KubeArchive client (or None when none is configured)."""
        if self.ka_context:
            try:
                client = _build_ka_client_from_context(self.ka_context)
            except ClusterError as exc:
                raise ClusterError(
                    f"requested KubeArchive context not in kubeconfig: {self.ka_context}"
                ) from exc
            logger.info("using KubeArchive context: %s", self.ka_context)
            return client
        host = _ka_conf_host(self.ka_conf)
        if host:
            logger.info("using KubeArchive API server: %s", host)
            return _build_ka_client_from_host(self.dyn.client.configuration, host)
        context = _find_ka_context()
        if context:
            logger.info("using KubeArchive context (auto): %s", context)
            return _build_ka_client_from_context(context)
        return None

    @property
    def _candidates(self) -> Iterator:
        """Yield (client, label) pairs: live cluster then KubeArchive."""
        yield self.dyn, "cluster"
        if self.ka_dyn is not None:
            yield self.ka_dyn, "kubearchive"

    @staticmethod
    def _get_from(client: Any, kind: str, namespace: str, name: str) -> dict:
        from kubernetes.client.rest import ApiException

        api_versions, kubernetes_kind = KIND_API[kind]
        last_error: Exception | None = None
        for api_version in api_versions:
            try:
                resource = client.resources.get(api_version=api_version, kind=kubernetes_kind)
            except Exception as exc:  # noqa: BLE001 - version not in discovery
                last_error = exc
                continue
            try:
                instance = client.get(resource, namespace=namespace, name=name)
                return _to_plain_dict(instance)
            except ApiException as exc:
                # Try the next api_version candidate.
                last_error = exc
                continue
        if last_error is not None:
            raise last_error
        raise ClusterError(f"no api_version available for {kind}")

    def get_object(self, kind: str, namespace: str, name: str) -> dict | None:
        """Fetch one object as a plain dict; None when not found anywhere.

        Raises ClusterError only when every backend failed with a non-404
        error (network/auth/API problem) - that is a fail-fast condition.
        """
        from kubernetes.client.rest import ApiException

        errors: list[str] = []
        not_found = False
        for client, label in self._candidates:
            try:
                return self._get_from(client, kind, namespace, name)
            except ApiException as exc:
                if exc.status == 404:
                    not_found = True
                    body = str(getattr(exc, "body", None) or "").strip()
                    detail = f": {body[:300]}" if body else ""
                    logger.debug(
                        "%s: %s %s/%s: not found%s",
                        label,
                        kind,
                        namespace,
                        name,
                        detail,
                    )
                    continue
                errors.append(f"{label}: HTTP {exc.status} {exc.reason}")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{label}: {exc}")
                continue
        if not_found:
            return None
        raise ClusterError(
            f"could not fetch {kind} {namespace}/{name}: "
            + ("; ".join(errors) if errors else "no backend available")
        )

    def get_logs(self, namespace: str, pod: str, container: str) -> str | None:
        """Return a container's logs, or None when unavailable (best-effort)."""
        from kubernetes.client.rest import ApiException

        try:
            return self.core.read_namespaced_pod_log(namespace, pod, container=container)
        except ApiException as exc:
            logger.debug("no logs for %s/%s %s: HTTP %d", namespace, pod, container, exc.status)
            return None
        except Exception as exc:  # noqa: BLE001
            logger.debug("no logs for %s/%s %s: %s", namespace, pod, container, exc)
            return None

    def wait_completed(
        self, namespace: str, name: str, timeout: float, interval: float = POLL_INTERVAL
    ) -> dict | None:
        """Poll until status.completionTime is set; None on timeout."""
        deadline = time.monotonic() + timeout
        while True:
            obj = self.get_object("pipelinerun", namespace, name)
            if obj is not None and (obj.get("status") or {}).get("completionTime"):
                return obj
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            time.sleep(min(interval, remaining))
