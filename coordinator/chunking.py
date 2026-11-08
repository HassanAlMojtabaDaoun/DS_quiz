"""Chunking strategies: fixed-size and content-defined (FastCDC).

Fixed-size chunking cuts every CHUNK_SIZE bytes. It is fast, but inserting a
single byte at the start of a file shifts every later boundary, so no chunk
of the edited file matches the original and deduplication is lost.

Content-defined chunking (CDC) chooses boundaries from the *content*: a
rolling "gear" hash is computed over the bytes and a boundary is declared
wherever the hash matches a bit mask. After an insertion the boundaries
re-synchronise with the original ones within one chunk, so all later chunks
deduplicate again.

FastCDC (Xia et al., USENIX ATC 2016) adds:
  * cut-point skipping: no boundary is searched before `min_size`;
  * normalized chunking: a stricter mask before the target average size and
    a looser one after it, which concentrates chunk sizes around the average;
  * a hard upper limit `max_size`.
"""

import random
from typing import BinaryIO, Iterator

MASK64 = (1 << 64) - 1

# Deterministic gear table: identical on every run and every machine, so the
# same content always produces the same chunk boundaries.
_rng = random.Random(0x5EED_FA57_CDC)
GEAR = [_rng.getrandbits(64) for _ in range(256)]

# Defaults keep every chunk <= 4 MiB, the storage node's maximum chunk size.
CDC_MIN_SIZE = 256 * 1024
CDC_AVG_SIZE = 1024 * 1024
CDC_MAX_SIZE = 4 * 1024 * 1024


def _high_mask(bits: int) -> int:
    """Mask on the *high* bits; the low bits of a gear hash mix poorly."""
    bits = max(1, min(bits, 63))
    return ((1 << bits) - 1) << (64 - bits)


def fixed_chunks(stream: BinaryIO, chunk_size: int) -> Iterator[bytes]:
    while True:
        block = stream.read(chunk_size)
        if not block:
            return
        yield block


def _cut_point(buf, min_size: int, avg_size: int, max_size: int,
               mask_s: int, mask_l: int) -> int:
    """Return the length of the next chunk at the start of buf."""
    n = len(buf)
    if n <= min_size:
        return n
    if n > max_size:
        n = max_size
    normal = min(avg_size, n)

    gear = GEAR
    h = 0
    i = min_size
    # Stricter mask before the average size ...
    while i < normal:
        h = ((h << 1) + gear[buf[i]]) & MASK64
        if not h & mask_s:
            return i + 1
        i += 1
    # ... looser mask after it.
    while i < n:
        h = ((h << 1) + gear[buf[i]]) & MASK64
        if not h & mask_l:
            return i + 1
        i += 1
    return n


def fastcdc_chunks(stream: BinaryIO,
                   min_size: int = CDC_MIN_SIZE,
                   avg_size: int = CDC_AVG_SIZE,
                   max_size: int = CDC_MAX_SIZE) -> Iterator[bytes]:
    if not (0 < min_size <= avg_size <= max_size):
        raise ValueError("Require 0 < min_size <= avg_size <= max_size")

    bits = avg_size.bit_length() - 1          # log2(average size)
    mask_s = _high_mask(bits + 2)             # normalized chunking, level 2
    mask_l = _high_mask(bits - 2)

    buf = bytearray()
    eof = False
    while True:
        # Keep at least max_size bytes buffered so a full search is possible.
        while not eof and len(buf) < max_size:
            data = stream.read(max_size)
            if not data:
                eof = True
            else:
                buf += data
        if not buf:
            return

        cut = _cut_point(buf, min_size, avg_size, max_size, mask_s, mask_l)
        yield bytes(buf[:cut])
        del buf[:cut]


def iter_chunks(stream: BinaryIO, method: str, chunk_size: int) -> Iterator[bytes]:
    if method == "fixed":
        return fixed_chunks(stream, chunk_size)
    if method == "cdc":
        return fastcdc_chunks(stream)
    raise ValueError(f"Unknown chunking method: {method}")


class StreamChunker:
    """Incremental chunker for streamed uploads.

    feed() takes the next piece of the request body (any size) and returns
    the chunks that are complete; finish() returns the rest. Memory use is
    bounded by one maximum-size chunk plus the incoming piece, so a 1 GB
    upload never has to be held in memory or on the coordinator's disk.
    The boundaries are identical to fixed_chunks() / fastcdc_chunks().
    """

    def __init__(self, method: str, chunk_size: int,
                 min_size: int = CDC_MIN_SIZE, avg_size: int = CDC_AVG_SIZE,
                 max_size: int = CDC_MAX_SIZE):
        if method not in ("fixed", "cdc"):
            raise ValueError(f"Unknown chunking method: {method}")
        self.method = method
        self.chunk_size = chunk_size
        self.min_size, self.avg_size, self.max_size = min_size, avg_size, max_size
        bits = avg_size.bit_length() - 1
        self.mask_s = _high_mask(bits + 2)
        self.mask_l = _high_mask(bits - 2)
        self.buf = bytearray()

    def _cut(self) -> int:
        if self.method == "fixed":
            return min(self.chunk_size, len(self.buf))
        return _cut_point(self.buf, self.min_size, self.avg_size,
                          self.max_size, self.mask_s, self.mask_l)

    def _window(self) -> int:
        return self.chunk_size if self.method == "fixed" else self.max_size

    def feed(self, data: bytes) -> list[bytes]:
        self.buf += data
        out = []
        # Only cut when a full search window is buffered, so boundaries do
        # not depend on how the network split the body.
        while len(self.buf) >= self._window():
            cut = self._cut()
            out.append(bytes(self.buf[:cut]))
            del self.buf[:cut]
        return out

    def finish(self) -> list[bytes]:
        out = []
        while self.buf:
            cut = self._cut()
            out.append(bytes(self.buf[:cut]))
            del self.buf[:cut]
        return out
