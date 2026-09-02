"""Shared test helpers for plrtool tests.

Builders for fake raw Kubernetes manifests (PipelineRun/TaskRun/Pod), stubs
for the cluster client, and a CLI-argv parser helper shared across modules.
"""

import plrtool


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
