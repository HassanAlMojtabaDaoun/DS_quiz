import itertools
import os

import pytest

from coordinator import erasure


@pytest.mark.parametrize("codec", sorted(erasure.CODECS))
@pytest.mark.parametrize("k,m", [(4, 2), (2, 1), (3, 3), (6, 3)])
def test_any_k_shards_rebuild_the_chunk(codec, k, m):
    data = os.urandom(10_000 + k)              # not a multiple of k: padding
    shards = erasure.encode(data, k, m, codec)
    assert len(shards) == k + m
    assert len({len(s) for s in shards}) == 1  # equal-sized shards
    for keep in itertools.combinations(range(k + m), k):
        available = {i: shards[i] for i in keep}
        assert erasure.decode(available, k, m, len(data), codec) == data


def test_code_is_systematic():
    data = os.urandom(4000)
    shards = erasure.encode(data, 4, 2, "pyrs")
    assert b"".join(shards[:4]) == data        # data shards = the data itself


def test_fewer_than_k_shards_fails():
    data = os.urandom(1000)
    shards = erasure.encode(data, 4, 2, "pyrs")
    with pytest.raises(ValueError):
        erasure.decode({0: shards[0], 5: shards[5], 3: shards[3]}, 4, 2,
                       len(data), "pyrs")


def test_storage_overhead():
    data = os.urandom(4 * 1024 * 1024)
    shards = erasure.encode(data, 4, 2)
    assert sum(map(len, shards)) / len(data) == pytest.approx(1.5, rel=1e-3)
