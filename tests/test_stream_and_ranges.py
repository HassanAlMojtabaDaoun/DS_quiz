import io
import random

import pytest

from coordinator.chunking import StreamChunker, fastcdc_chunks, fixed_chunks
from coordinator.ranges import (RangeNotSatisfiable, chunks_for_range,
                                parse_range)

DATA = random.Random(3).randbytes(9 * 1024 * 1024 + 123)


def stream(method, piece):
    ch = StreamChunker(method, 4 << 20)
    out = []
    for i in range(0, len(DATA), piece):
        out += ch.feed(DATA[i:i + piece])
    return out + ch.finish()


@pytest.mark.parametrize("piece", [1000, 65536, 5_000_000])
def test_stream_chunker_matches_file_chunker(piece):
    assert stream("fixed", piece) == list(fixed_chunks(io.BytesIO(DATA), 4 << 20))
    assert stream("cdc", piece) == list(fastcdc_chunks(io.BytesIO(DATA)))


@pytest.mark.parametrize("header,expected", [
    ("bytes=0-99", (0, 99)),
    ("bytes=100-", (100, 999)),
    ("bytes=-50", (950, 999)),
    ("bytes=900-5000", (900, 999)),
    (None, None),
    ("items=0-1", None),
])
def test_parse_range(header, expected):
    assert parse_range(header, 1000) == expected


@pytest.mark.parametrize("header", ["bytes=1000-", "bytes=5-2", "bytes=-0"])
def test_unsatisfiable(header):
    with pytest.raises(RangeNotSatisfiable):
        parse_range(header, 1000)


def test_chunks_for_range():
    sizes = [100, 100, 100]
    assert list(chunks_for_range(sizes, 150, 249)) == [(1, 50, 100), (2, 0, 50)]
    assert list(chunks_for_range(sizes, 0, 0)) == [(0, 0, 1)]
    data = bytes(range(256)) + bytes(44)
    parts = [data[0:100], data[100:200], data[200:300]]
    got = b"".join(parts[i][a:b] for i, a, b in chunks_for_range(sizes, 37, 233))
    assert got == data[37:234]
