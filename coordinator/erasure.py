"""Reed-Solomon erasure coding (k data shards + m parity shards).

A chunk is split into k equal data shards and m parity shards are computed
from them. ANY k of the k+m shards are enough to rebuild the chunk, so the
chunk survives the loss of any m shards, at a storage cost of (k+m)/k
(1.5x for 4+2) instead of 3x for triple replication.

Two interchangeable backends:
  * "zfec"  - the zfec C library (used when it is installed and passes a
              self-test at start-up);
  * "pyrs"  - a built-in pure-Python Reed-Solomon codec over GF(2^8), using
              a systematic Cauchy matrix. It is fast because multiplying a
              whole shard by a constant is a single bytes.translate() call
              and XOR of two shards is one big-integer XOR.

The two backends produce different parity bytes, so the backend name is
stored with every erasure-coded chunk and decoding uses the same backend.
"""

import os

# ---------------------------------------------------------------------------
# GF(2^8) arithmetic (primitive polynomial x^8 + x^4 + x^3 + x^2 + 1 = 0x11d)
# ---------------------------------------------------------------------------

_EXP = [0] * 512
_LOG = [0] * 256
_x = 1
for _i in range(255):
    _EXP[_i] = _x
    _LOG[_x] = _i
    _x <<= 1
    if _x & 0x100:
        _x ^= 0x11D
for _i in range(255, 512):
    _EXP[_i] = _EXP[_i - 255]


def gf_mul(a: int, b: int) -> int:
    if a == 0 or b == 0:
        return 0
    return _EXP[_LOG[a] + _LOG[b]]


def gf_inv(a: int) -> int:
    if a == 0:
        raise ZeroDivisionError("0 has no inverse in GF(256)")
    return _EXP[255 - _LOG[a]]


# _MUL[c] maps every byte b to c*b, usable directly with bytes.translate().
_MUL = [bytes(gf_mul(c, b) for b in range(256)) for c in range(256)]


def _scale(shard: bytes, c: int) -> int:
    """c * shard, returned as a big integer (for fast XOR accumulation)."""
    if c == 0:
        return 0
    data = shard if c == 1 else shard.translate(_MUL[c])
    return int.from_bytes(data, "little")


def _combine(coeffs, shards, size: int) -> bytes:
    acc = 0
    for c, s in zip(coeffs, shards):
        acc ^= _scale(s, c)
    return acc.to_bytes(size, "little")


def _cauchy_row(j: int, k: int) -> list[int]:
    """Coefficients of parity shard j: 1 / (x_j + y_i), x_j = k + j, y_i = i.

    Every square sub-matrix of [identity; Cauchy] is invertible, which is
    exactly the property that lets ANY k shards rebuild the data (MDS code).
    """
    return [gf_inv((k + j) ^ i) for i in range(k)]


def _invert(matrix: list[list[int]]) -> list[list[int]]:
    """Gauss-Jordan inversion over GF(256)."""
    n = len(matrix)
    a = [row[:] + [int(i == r) for i in range(n)] for r, row in enumerate(matrix)]
    for col in range(n):
        pivot = next(r for r in range(col, n) if a[r][col])
        a[col], a[pivot] = a[pivot], a[col]
        inv = gf_inv(a[col][col])
        a[col] = [gf_mul(v, inv) for v in a[col]]
        for r in range(n):
            if r != col and a[r][col]:
                f = a[r][col]
                a[r] = [v ^ gf_mul(f, p) for v, p in zip(a[r], a[col])]
    return [row[n:] for row in a]


# ---------------------------------------------------------------------------
# Common helpers
# ---------------------------------------------------------------------------

def split(data: bytes, k: int) -> tuple[list[bytes], int]:
    """Split into k equal shards (zero padded). Returns (shards, shard_size)."""
    shard_size = max(1, -(-len(data) // k))
    padded = data + b"\0" * (shard_size * k - len(data))
    return [padded[i * shard_size:(i + 1) * shard_size] for i in range(k)], shard_size


def _check(k: int, m: int) -> None:
    if k < 1 or m < 0 or k + m > 255:
        raise ValueError("Require k >= 1, m >= 0 and k + m <= 255")


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------

class PyRS:
    name = "pyrs"

    def encode(self, data: bytes, k: int, m: int) -> list[bytes]:
        _check(k, m)
        shards, size = split(data, k)
        parity = [_combine(_cauchy_row(j, k), shards, size) for j in range(m)]
        return shards + parity

    def decode(self, available: dict[int, bytes], k: int, m: int,
               length: int) -> bytes:
        _check(k, m)
        if len(available) < k:
            raise ValueError(f"Need {k} shards, only {len(available)} available")
        if all(i in available for i in range(k)):          # fast path
            return b"".join(available[i] for i in range(k))[:length]

        use = sorted(available)[:k]
        rows = [[int(c == i) for c in range(k)] if i < k else _cauchy_row(i - k, k)
                for i in use]
        inv = _invert(rows)
        shards = [available[i] for i in use]
        size = len(shards[0])
        data = [available[i] if i in available else _combine(inv[i], shards, size)
                for i in range(k)]
        return b"".join(data)[:length]


class Zfec:
    name = "zfec"

    def __init__(self):
        import zfec  # noqa: F401  (ImportError -> backend unavailable)
        self._zfec = zfec

    def encode(self, data: bytes, k: int, m: int) -> list[bytes]:
        _check(k, m)
        shards, _ = split(data, k)
        out = self._zfec.Encoder(k, k + m).encode(shards)
        return [bytes(s) for s in out]

    def decode(self, available: dict[int, bytes], k: int, m: int,
               length: int) -> bytes:
        _check(k, m)
        if len(available) < k:
            raise ValueError(f"Need {k} shards, only {len(available)} available")
        nums = sorted(available)[:k]
        blocks = self._zfec.Decoder(k, k + m).decode(
            [available[i] for i in nums], nums)
        return b"".join(bytes(b) for b in blocks)[:length]


def _self_test(codec) -> bool:
    try:
        data = os.urandom(10_000)
        shards = codec.encode(data, 4, 2)
        if len(shards) != 6:
            return False
        for lost in ((0, 1), (2, 5), (4, 5), (1, 3)):
            avail = {i: s for i, s in enumerate(shards) if i not in lost}
            if codec.decode(avail, 4, 2, len(data)) != data:
                return False
        return True
    except Exception:
        return False


CODECS = {"pyrs": PyRS()}
try:
    _z = Zfec()
    if _self_test(_z):
        CODECS["zfec"] = _z
except ImportError:
    pass

_preferred = os.getenv("EC_BACKEND", "zfec")
DEFAULT_CODEC = _preferred if _preferred in CODECS else "pyrs"


def get_codec(name: str | None = None):
    name = name or DEFAULT_CODEC
    if name not in CODECS:
        raise RuntimeError(f"Erasure-coding backend '{name}' is not available")
    return CODECS[name]


def encode(data: bytes, k: int, m: int, codec: str | None = None) -> list[bytes]:
    return get_codec(codec).encode(data, k, m)


def decode(available: dict[int, bytes], k: int, m: int, length: int,
           codec: str | None = None) -> bytes:
    return get_codec(codec).decode(available, k, m, length)
