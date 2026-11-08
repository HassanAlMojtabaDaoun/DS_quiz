"""Consistent hashing ring with virtual nodes.

Every physical node is placed on a 2^64 ring at `vnodes` pseudo-random
positions ("virtual nodes"). A chunk is placed by hashing its address onto
the same ring and walking clockwise: the first `n` *distinct* physical nodes
met form its replica set (its "preference list").

Why it matters:
  * Adding or removing a node only changes ownership of the arcs next to that
    node's virtual positions, so only ~1/N of the data moves (instead of
    ~(N-1)/N with naive `hash % N` placement).
  * Many virtual nodes per physical node smooth out the load: each node owns
    many small arcs rather than one large arc.
"""

import bisect
import hashlib

DEFAULT_VNODES = 128


def ring_hash(value: str) -> int:
    """Position on the ring: first 8 bytes of SHA-256, as an integer."""
    return int.from_bytes(hashlib.sha256(value.encode()).digest()[:8], "big")


class ConsistentHashRing:
    def __init__(self, nodes=(), vnodes: int = DEFAULT_VNODES):
        if vnodes < 1:
            raise ValueError("vnodes must be >= 1")
        self.vnodes = vnodes
        self._positions: list[int] = []   # sorted ring positions
        self._owners: list[str] = []      # owner node of each position
        self._nodes: set[str] = set()
        for node in nodes:
            self.add_node(node)

    # -- membership ---------------------------------------------------------

    @property
    def nodes(self) -> list[str]:
        return sorted(self._nodes)

    def add_node(self, node: str) -> None:
        if node in self._nodes:
            return
        self._nodes.add(node)
        for i in range(self.vnodes):
            pos = ring_hash(f"{node}#vnode-{i}")
            idx = bisect.bisect_left(self._positions, pos)
            self._positions.insert(idx, pos)
            self._owners.insert(idx, node)

    def remove_node(self, node: str) -> None:
        if node not in self._nodes:
            return
        self._nodes.discard(node)
        keep = [(p, o) for p, o in zip(self._positions, self._owners)
                if o != node]
        self._positions = [p for p, _ in keep]
        self._owners = [o for _, o in keep]

    # -- lookup -------------------------------------------------------------

    def get_nodes(self, key: str, n: int) -> list[str]:
        """Preference list: first n distinct physical nodes clockwise of key."""
        if not self._nodes:
            raise RuntimeError("Ring is empty")
        n = min(n, len(self._nodes))
        # For a hex SHA-256 chunk address, use its own leading bits directly.
        try:
            pos = int(key[:16], 16) if len(key) == 64 else ring_hash(key)
        except ValueError:
            pos = ring_hash(key)

        start = bisect.bisect_right(self._positions, pos)
        result: list[str] = []
        total = len(self._positions)
        for step in range(total):
            owner = self._owners[(start + step) % total]
            if owner not in result:
                result.append(owner)
                if len(result) == n:
                    break
        return result

    def ownership(self) -> dict[str, float]:
        """Fraction of the ring (primary ownership) held by each node."""
        space = 2 ** 64
        share = {node: 0 for node in self._nodes}
        for i, pos in enumerate(self._positions):
            prev = self._positions[i - 1] if i else self._positions[-1] - space
            share[self._owners[i]] += pos - prev
        return {node: share[node] / space for node in sorted(share)}


def modulo_placement(key: str, nodes: list[str], n: int) -> list[str]:
    """Naive placement used only as a baseline: hash(key) % N."""
    ordered = sorted(nodes)
    start = int(key[:16], 16) % len(ordered)
    return [ordered[(start + i) % len(ordered)]
            for i in range(min(n, len(ordered)))]
