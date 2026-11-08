import hashlib

from coordinator.ring import ConsistentHashRing

NODES = ["node1", "node2", "node3", "node4"]
KEYS = [hashlib.sha256(str(i).encode()).hexdigest() for i in range(5000)]


def test_replicas_are_distinct_nodes():
    ring = ConsistentHashRing(NODES)
    for k in KEYS[:500]:
        nodes = ring.get_nodes(k, 3)
        assert len(nodes) == 3 and len(set(nodes)) == 3


def test_placement_is_deterministic():
    a, b = ConsistentHashRing(NODES), ConsistentHashRing(reversed(NODES))
    assert all(a.get_nodes(k, 3) == b.get_nodes(k, 3) for k in KEYS[:500])


def test_adding_a_node_moves_about_one_fifth():
    before = ConsistentHashRing(NODES)
    after = ConsistentHashRing(NODES + ["node5"])
    moved = sum(before.get_nodes(k, 1) != after.get_nodes(k, 1) for k in KEYS)
    assert 0.12 < moved / len(KEYS) < 0.28          # theory: 1/5
    # Every key that moved must have moved to the new node.
    assert all(after.get_nodes(k, 1) == ["node5"]
               for k in KEYS if before.get_nodes(k, 1) != after.get_nodes(k, 1))


def test_ownership_sums_to_one_and_is_balanced():
    share = ConsistentHashRing(NODES, vnodes=128).ownership()
    assert abs(sum(share.values()) - 1) < 1e-9
    assert all(0.15 < s < 0.35 for s in share.values())
