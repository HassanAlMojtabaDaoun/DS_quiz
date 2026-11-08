import hashlib
import io
import random

from coordinator.chunking import fastcdc_chunks, fixed_chunks, CDC_MAX_SIZE

DATA = random.Random(1).randbytes(12 * 1024 * 1024)


def digests(chunks):
    return [hashlib.sha256(c).hexdigest() for c in chunks]


def test_chunks_reassemble_exactly():
    assert b"".join(fastcdc_chunks(io.BytesIO(DATA))) == DATA
    assert b"".join(fixed_chunks(io.BytesIO(DATA), 4 << 20)) == DATA


def test_cdc_respects_max_size():
    assert max(map(len, fastcdc_chunks(io.BytesIO(DATA)))) <= CDC_MAX_SIZE


def test_cdc_resynchronises_after_insertion():
    original = set(digests(fastcdc_chunks(io.BytesIO(DATA))))
    edited = digests(fastcdc_chunks(io.BytesIO(b"inserted" + DATA)))
    reused = sum(d in original for d in edited)
    assert reused >= len(edited) - 2

    fixed_orig = set(digests(fixed_chunks(io.BytesIO(DATA), 4 << 20)))
    fixed_edit = digests(fixed_chunks(io.BytesIO(b"inserted" + DATA), 4 << 20))
    assert sum(d in fixed_orig for d in fixed_edit) == 0
