import hashlib
from collections import Counter

import pytest
from fastapi import HTTPException

from coordinator import cluster
from coordinator.ring import ConsistentHashRing

NODES = {f"node{i}": f"http://n{i}" for i in range(1, 5)}


@pytest.fixture(autouse=True)
def four_node_ring(monkeypatch):
    monkeypatch.setattr(cluster, "NODES", NODES)
    monkeypatch.setattr(cluster, "RING", ConsistentHashRing(NODES))
    monkeypatch.setattr(cluster, "failed_nodes", lambda force=False: frozenset())


def test_ec_4_plus_2_on_4_nodes_survives_any_single_node_loss():
    for i in range(200):
        digest = hashlib.sha256(str(i).encode()).hexdigest()
        placement = cluster.ec_assignment(digest, 4, 2)
        assert len(placement) == 6
        per_node = Counter(placement)
        assert max(per_node.values()) <= 2     # lose a node -> lose <= m shards


def test_ec_refuses_when_too_few_nodes():
    with pytest.raises(HTTPException):
        cluster.ec_assignment("0" * 64, 4, 2, candidates=["node1", "node2"])
