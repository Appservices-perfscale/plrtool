"""RunGraph: assembly seam for the PLR → TaskRun → Pod record graph.

Both provenance paths build the same graph through the same interface:

- offline: ``CacheStore.load()`` links every archived object;
- online: ``download._fetch_details()`` links what it just fetched.

One seam keeps the in-memory and offline pictures of the same cache from
drifting apart, and concentrates the linking/dedup/ordering rules (the Run
Graph concept, see CONTEXT.md) in a single module instead of two.
"""

from __future__ import annotations

from .records import PLRRecord, PodRecord, TaskRunRecord

__all__ = [
    "link_plr_taskruns",
    "link_run_graph",
]
# ---------------------------------------------------------------------------
# RunGraph: the PLR -> TaskRun -> Pod aggregation seam
# ---------------------------------------------------------------------------


def link_plr_taskruns(
    plr: PLRRecord,
    taskruns: dict[str, TaskRunRecord],
    pods: dict[str, PodRecord],
) -> PLRRecord:
    """Attach a PLR's child TaskRuns and their Pods. Returns the PLR.

    Every name in ``plr.tr_refs`` present in the taskruns map is attached, in
    reference order; each TaskRun that names a Pod is linked to it.  Names
    absent from the maps (a fetch that never happened) are simply not linked,
    not an error.  Idempotent: re-linking rebuilds the same list, so repeated
    calls cannot duplicate records.
    """
    plr.taskruns = [taskruns[name] for name in plr.tr_refs if name in taskruns]
    for tr in plr.taskruns:
        if tr.pod_name and tr.pod_name in pods:
            tr.pod = pods[tr.pod_name]
    return plr


def link_run_graph(
    plrs: dict[str, PLRRecord],
    taskruns: dict[str, TaskRunRecord],
    pods: dict[str, PodRecord],
) -> dict[str, PLRRecord]:
    """Link every PLR in the map onto its TaskRuns and Pods. Returns the map."""
    for plr in plrs.values():
        link_plr_taskruns(plr, taskruns, pods)
    return plrs
