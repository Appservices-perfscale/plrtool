"""Unit tests for the RunGraph assembly seam (graph.py + log_containers)."""

from plrtool import CacheStore
from plrtool.graph import link_plr_taskruns, link_run_graph

from helpers import raw_pod, raw_plr, raw_taskrun


def test_link_run_graph_attaches_trs_and_pods_in_ref_order(tmp_path):
    store = CacheStore(tmp_path)
    store.add_plr(raw_plr("p", refs=["tr-b", "tr-a"]))
    store.add_taskrun(raw_taskrun("tr-b", pod="pod-b"))
    store.add_taskrun(raw_taskrun("tr-a", pod="pod-a"))
    store.add_pod(raw_pod("pod-a"))
    store.add_pod(raw_pod("pod-b"))
    link_run_graph(store.plrs, store.taskruns, store.pods)
    plr = store.plrs["p"]
    assert [tr.name for tr in plr.taskruns] == ["tr-b", "tr-a"]
    assert [tr.pod.name for tr in plr.taskruns] == ["pod-b", "pod-a"]


def test_link_skips_trs_and_pods_not_in_maps(tmp_path):
    store = CacheStore(tmp_path)
    store.add_plr(raw_plr("p", refs=["present", "absent"]))
    store.add_taskrun(raw_taskrun("present", pod="pod-x"))
    store.add_taskrun(raw_taskrun("absent", pod="pod-z"))
    link_plr_taskruns(store.plrs["p"], store.taskruns, store.pods)
    plr = store.plrs["p"]
    # both refs are archived -> both linked, even though a Pod is missing
    assert [tr.name for tr in plr.taskruns] == ["present", "absent"]
    assert plr.taskruns[0].pod is None
    assert plr.taskruns[1].pod is None


def test_link_is_idempotent(tmp_path):
    store = CacheStore(tmp_path)
    store.add_plr(raw_plr("p", refs=["tr-1"]))
    store.add_taskrun(raw_taskrun("tr-1", pod="pod-1"))
    store.add_pod(raw_pod("pod-1"))
    link_run_graph(store.plrs, store.taskruns, store.pods)
    first = list(store.plrs["p"].taskruns)
    link_run_graph(store.plrs, store.taskruns, store.pods)
    assert list(store.plrs["p"].taskruns) == first
    assert len(first) == 1


def test_offline_load_matches_collected_graph(tmp_path):
    # Regression the seam exists for: online collection and offline load must
    # produce the identical Run Graph for the same archived objects.
    store = CacheStore(tmp_path)
    store.add_plr(raw_plr("p", refs=["tr-1"]))
    store.add_taskrun(raw_taskrun("tr-1", pod="pod-1"))
    store.add_pod(raw_pod("pod-1"))
    link_plr_taskruns(store.plrs["p"], store.taskruns, store.pods)

    reloaded = CacheStore(tmp_path).load()
    assert [tr.name for tr in reloaded.plrs["p"].taskruns] == ["tr-1"]
    assert reloaded.plrs["p"].taskruns[0].pod is reloaded.pods["pod-1"]
    # and the online graph itself matches the offline one
    assert [tr.pod is not None for tr in store.plrs["p"].taskruns] == [
        tr.pod is not None for tr in reloaded.plrs["p"].taskruns
    ]


def test_log_containers_are_steps_then_sidecars(tmp_path):
    store = CacheStore(tmp_path)
    store.add_taskrun(
        raw_taskrun("t", steps=[("s1", "2026-08-13T10:00:10Z", "2026-08-13T10:00:15Z")])
    )
    tr = store.taskruns["t"]
    assert tr.log_containers == ["step-s1"]
    tr.sidecars = ["sc-1"]
    assert tr.log_containers == ["step-s1", "sc-1"]
