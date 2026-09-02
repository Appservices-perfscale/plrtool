"""Records: subset of the cached data kept in memory.

Dataclasses (``PLRRecord``/``TaskRunRecord``/``PodRecord``, plus step/container
sub-records) are built by the ``parse_*`` functions from raw manifests, so
analysis code never touches raw JSON shapes.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

from .utils import parse_ts_dt

__all__ = [
    "ContainerStatusRecord",
    "PLRRecord",
    "PodRecord",
    "StepRecord",
    "TaskRunRecord",
    "parse_plr",
    "parse_pod",
    "parse_taskrun",
    "succeeded_condition",
]
# ---------------------------------------------------------------------------
# 4. Records: subset of the cached data kept in memory
# ---------------------------------------------------------------------------


@dataclass
class StepRecord:
    """One step of a TaskRun (subset of status.steps[].terminated)."""

    name: str | None = None
    container: str | None = None
    started_at: dt.datetime | None = None
    finished_at: dt.datetime | None = None
    reason: str | None = None
    exit_code: int | None = None
    message: str | None = None


@dataclass
class ContainerStatusRecord:
    """One container status of a Pod (subset of containerStatuses[])."""

    name: str
    terminated_reason: str | None = None
    terminated_started: dt.datetime | None = None
    terminated_finished: dt.datetime | None = None
    terminated_exit_code: int | None = None
    waiting_reason: str | None = None


@dataclass
class PodRecord:
    """A Pod (subset of the Pod manifest)."""

    name: str
    namespace: str = ""
    node: str | None = None
    phase: str | None = None
    conditions: list = field(default_factory=list)  # (type, status, reason, time)
    containers: list[ContainerStatusRecord] = field(default_factory=list)


@dataclass
class TaskRunRecord:
    """A TaskRun (subset of the TaskRun manifest)."""

    name: str
    namespace: str = ""
    pipeline_task: str | None = None
    created: dt.datetime | None = None
    started: dt.datetime | None = None
    completed: dt.datetime | None = None
    succeeded_status: str | None = None
    succeeded_reason: str | None = None
    succeeded_message: str | None = None
    pod_name: str | None = None
    pod: PodRecord | None = None
    steps: list[StepRecord] = field(default_factory=list)
    sidecars: list = field(default_factory=list)  # container names


@dataclass
class PLRRecord:
    """A PipelineRun (subset of the PipelineRun manifest)."""

    name: str
    namespace: str = ""
    pipeline: str | None = None
    created: dt.datetime | None = None
    started: dt.datetime | None = None
    finally_start: dt.datetime | None = None
    completed: dt.datetime | None = None
    succeeded_status: str | None = None
    succeeded_reason: str | None = None
    succeeded_message: str | None = None
    tr_refs: list = field(default_factory=list)  # TaskRun names from childReferences
    taskruns: list[TaskRunRecord] = field(default_factory=list)
    skipped_stopping: int = 0  # task count skipped with "PipelineRun was stopping"


def succeeded_condition(item: dict) -> dict:
    """Return the status.conditions entry of type Succeeded (or {})."""
    for condition in (item.get("status") or {}).get("conditions") or []:
        if condition.get("type") == "Succeeded":
            return condition
    return {}


def parse_plr(raw: dict, namespace: str | None = None) -> PLRRecord:
    """Build a PLRRecord from a raw PipelineRun manifest."""
    meta = raw.get("metadata") or {}
    status = raw.get("status") or {}
    condition = succeeded_condition(raw)
    refs = [
        r.get("name")
        for r in status.get("childReferences") or []
        if r.get("kind") == "TaskRun" and r.get("name")
    ]
    skipped = sum(
        1
        for task in status.get("skippedTasks") or []
        if task.get("reason") == "PipelineRun was stopping"
    )
    return PLRRecord(
        name=meta.get("name") or "unknown",
        namespace=meta.get("namespace") or namespace or "",
        pipeline=(meta.get("labels") or {}).get("tekton.dev/pipeline"),
        created=parse_ts_dt(meta.get("creationTimestamp")),
        started=parse_ts_dt(status.get("startTime")),
        finally_start=parse_ts_dt(status.get("finallyStartTime")),
        completed=parse_ts_dt(status.get("completionTime")),
        succeeded_status=condition.get("status"),
        succeeded_reason=condition.get("reason"),
        succeeded_message=condition.get("message"),
        tr_refs=refs,
        skipped_stopping=skipped,
    )


def parse_taskrun(raw: dict, namespace: str | None = None) -> TaskRunRecord:
    """Build a TaskRunRecord from a raw TaskRun manifest."""
    meta = raw.get("metadata") or {}
    status = raw.get("status") or {}
    condition = succeeded_condition(raw)
    steps = []
    for step in status.get("steps") or []:
        terminated = step.get("terminated") or {}
        steps.append(
            StepRecord(
                name=step.get("name"),
                container=step.get("container"),
                started_at=parse_ts_dt(terminated.get("startedAt")),
                finished_at=parse_ts_dt(terminated.get("finishedAt")),
                reason=terminated.get("reason"),
                exit_code=terminated.get("exitCode"),
                message=terminated.get("message"),
            )
        )
    sidecars = [s.get("container") for s in status.get("sidecars") or [] if s.get("container")]
    return TaskRunRecord(
        name=meta.get("name") or "unknown",
        namespace=meta.get("namespace") or namespace or "",
        pipeline_task=(meta.get("labels") or {}).get("tekton.dev/pipelineTask"),
        created=parse_ts_dt(meta.get("creationTimestamp")),
        started=parse_ts_dt(status.get("startTime")),
        completed=parse_ts_dt(status.get("completionTime")),
        succeeded_status=condition.get("status"),
        succeeded_reason=condition.get("reason"),
        succeeded_message=condition.get("message"),
        pod_name=status.get("podName"),
        steps=steps,
        sidecars=sidecars,
    )


def parse_pod(raw: dict, namespace: str | None = None) -> PodRecord:
    """Build a PodRecord from a raw Pod manifest."""
    meta = raw.get("metadata") or {}
    status = raw.get("status") or {}
    conditions = [
        (
            c.get("type"),
            c.get("status"),
            c.get("reason"),
            c.get("lastTransitionTime"),
        )
        for c in status.get("conditions") or []
    ]
    containers = []
    for container in (status.get("initContainerStatuses") or []) + (
        status.get("containerStatuses") or []
    ):
        state = container.get("state") or {}
        terminated = state.get("terminated") or {}
        waiting = state.get("waiting") or {}
        containers.append(
            ContainerStatusRecord(
                name=container.get("name"),
                terminated_reason=terminated.get("reason"),
                terminated_started=parse_ts_dt(terminated.get("startedAt")),
                terminated_finished=parse_ts_dt(terminated.get("finishedAt")),
                terminated_exit_code=terminated.get("exitCode"),
                waiting_reason=waiting.get("reason"),
            )
        )
    return PodRecord(
        name=meta.get("name") or "unknown",
        namespace=meta.get("namespace") or namespace or "",
        node=(raw.get("spec") or {}).get("nodeName"),
        phase=status.get("phase"),
        conditions=conditions,
        containers=containers,
    )
