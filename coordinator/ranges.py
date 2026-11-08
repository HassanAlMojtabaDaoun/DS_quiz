"""HTTP byte-range requests (RFC 9110, single range).

  bytes=0-99     first 100 bytes
  bytes=100-     from byte 100 to the end
  bytes=-500     last 500 bytes
"""

import re

_RANGE = re.compile(r"^\s*bytes\s*=\s*(\d*)\s*-\s*(\d*)\s*$")


class RangeNotSatisfiable(Exception):
    pass


def parse_range(header: str | None, size: int) -> tuple[int, int] | None:
    """Return (start, end) inclusive, or None for "whole object".

    Raises RangeNotSatisfiable (HTTP 416) for ranges outside the object.
    Malformed or multi-range headers are ignored (whole object), as the
    RFC allows.
    """
    if not header:
        return None
    m = _RANGE.match(header)
    if not m or "," in header:
        return None
    first, last = m.groups()
    if first == "" and last == "":
        return None
    if first == "":                         # suffix range: last N bytes
        n = int(last)
        if n == 0 or size == 0:
            raise RangeNotSatisfiable()
        return max(0, size - n), size - 1
    start = int(first)
    end = int(last) if last else size - 1
    if start >= size or end < start:
        raise RangeNotSatisfiable()
    return start, min(end, size - 1)


def chunks_for_range(sizes: list[int], start: int, end: int):
    """Yield (chunk_index, slice_start, slice_end) covering [start, end]."""
    offset = 0
    for i, size in enumerate(sizes):
        chunk_start, chunk_end = offset, offset + size - 1
        offset += size
        if chunk_end < start:
            continue
        if chunk_start > end:
            break
        yield i, max(start, chunk_start) - chunk_start, \
            min(end, chunk_end) - chunk_start + 1
