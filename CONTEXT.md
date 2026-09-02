# plrtool domain glossary (CONTEXT)

Terms an architecture review should use. Kept current as refactors deepen or
rename modules.

## Domain concepts

- **PLR (PipelineRun)** — a Tekton PipelineRun; the top-level unit the tool
  downloads, waits on, and analyzes. Identified by namespace + name.
- **TaskRun (TR)** — a Tekton TaskRun created under a PLR. A PLR lists its
  child TaskRuns in `status.childReferences`.
- **Pod** — the Pod a TaskRun runs in; a TR names it via `status.podName`.
- **Run Graph** — the assembled PLR → TaskRun → Pod linkage over the in-memory
  records. Assembly lives in ONE place: the `graph.py` seam
  (`link_run_graph` / `link_plr_taskruns`), shared by the online collector and
  the offline load, so the two provenance paths cannot produce differing
  pictures of the same cache.
- **Cache / Archive** — the on-disk JSON (plus legacy YAML) dump of every
  fetched object and one log file per container; the source of truth for
  offline analysis.
- **Record** — the in-memory subset (`PLRRecord` / `TaskRunRecord` /
  `PodRecord`) extracted from a raw manifest by `records.parse_*`.
- **Collection** — fetching a PLR (and optionally its details) from cluster or
  KubeArchive into the cache, cached-wins.
- **Target / Selector** — a single (namespace, plr) pair the tool operates on;
  selectors (`--namespace/--plr`, `--csv`) provide the targets.

## Architecture vocabulary

Uses the `/codebase-design` vocabulary: *module, interface, depth, seam,
adapter, leverage, locality.*
