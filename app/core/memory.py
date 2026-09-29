"""Process memory helpers.

CPython returns freed blocks to its own allocator, not always to the OS, so a
burst of short-lived allocations (proxy health checks, file hashing, JSON
payloads) inflates RSS long after the data is gone. ``trim_memory`` asks glibc
to hand the free arenas back, which keeps the idle footprint close to the
amount of data actually retained.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import gc
import sys

from app.core.logging import logger

_malloc_trim = None
_malloc_trim_resolved = False


def _resolve_malloc_trim():
    """Look up ``malloc_trim`` once. Returns ``None`` on non-glibc platforms."""
    global _malloc_trim, _malloc_trim_resolved
    if _malloc_trim_resolved:
        return _malloc_trim
    _malloc_trim_resolved = True
    if not sys.platform.startswith("linux"):
        return None
    try:
        libc_name = ctypes.util.find_library("c") or "libc.so.6"
        libc = ctypes.CDLL(libc_name)
        trim = libc.malloc_trim
        trim.argtypes = [ctypes.c_size_t]
        trim.restype = ctypes.c_int
    except (OSError, AttributeError):
        return None
    _malloc_trim = trim
    return _malloc_trim


def trim_memory(*, collect: bool = True) -> bool:
    """Release free allocator arenas back to the OS.

    Returns True when glibc actually returned memory. A no-op on platforms
    without ``malloc_trim``.
    """
    if collect:
        gc.collect()
    trim = _resolve_malloc_trim()
    if trim is None:
        return False
    try:
        return bool(trim(0))
    except OSError:
        return False


def freeze_after_startup() -> None:
    """Move startup-time objects out of the GC's generational scan.

    Imported modules, ORM metadata and route tables live for the whole process
    lifetime. ``gc.freeze`` keeps the collector from walking them on every
    generational pass, which lowers idle CPU and avoids touching those pages.
    """
    gc.collect()
    gc.freeze()
    trim_memory(collect=False)
    logger.debug("GC frozen: {} permanent objects", gc.get_freeze_count())


def current_rss_mb() -> int | None:
    """Resident set size in MB, or None where /proc is unavailable."""
    try:
        with open("/proc/self/status", "r", encoding="ascii") as status:
            for line in status:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) // 1024
    except OSError:
        return None
    return None
