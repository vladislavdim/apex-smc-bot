"""Keep disposable backup file pages out of the worker's memory budget."""
from __future__ import annotations

from contextlib import contextmanager
import logging
import os
import time
import threading

_WINDOW = 8 * 1024 * 1024


def discard_file_cache(path: str, *, sync: bool = False) -> None:
    """Best-effort OS hint; never changes file contents or backup validity."""
    if not hasattr(os, 'posix_fadvise'):
        return
    try:
        fd = os.open(path, os.O_RDWR if sync else os.O_RDONLY)
        try:
            if sync:
                os.fdatasync(fd)
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)
    except OSError:
        pass


def copy_stream(source, target) -> None:
    """Copy/compress in bounded chunks and release consumed file pages."""
    copied = 0
    while True:
        chunk = source.read(1024 * 1024)
        if not chunk:
            break
        target.write(chunk)
        copied += len(chunk)
        if copied >= _WINDOW:
            copied = 0
            target.flush()
            for stream, sync in ((source, False), (target, True)):
                try:
                    fd = stream.fileno()
                    if sync:
                        os.fdatasync(fd)
                    if hasattr(os, 'posix_fadvise'):
                        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                except (AttributeError, OSError):
                    pass


def sqlite_snapshot(source, target, source_path: str, target_path: str) -> None:
    copied_pages = 0
    def progress(status, remaining, total):
        nonlocal copied_pages
        current = total - remaining
        if current - copied_pages >= 2048 or remaining == 0:
            copied_pages = current
            discard_file_cache(target_path, sync=True)
            discard_file_cache(source_path)
    source.backup(target, pages=256, progress=progress)


def _file_and_anon_bytes():
    for path in ('/sys/fs/cgroup/memory.stat', '/sys/fs/cgroup/memory/memory.stat'):
        try:
            with open(path, encoding='utf-8') as source:
                values = {parts[0]: int(parts[1]) for line in source if len(parts := line.split()) == 2}
            return values.get('anon', values.get('total_rss', 0)), values.get('file', values.get('total_cache', 0))
        except (OSError, ValueError):
            continue
    return 0, 0


@contextmanager
def snapshot_stage(name: str, *paths: str):
    """Measure the whole container, including transient Git subprocess peaks."""
    from apex.ops.resource_guard import memory_snapshot
    started = time.monotonic()
    peak = {'rss': 0, 'service': 0, 'working_set': 0, 'anon': 0, 'file': 0}
    stopped = threading.Event()
    def sample():
        value = memory_snapshot()
        anon, file_bytes = _file_and_anon_bytes()
        current = dict(rss=value.rss_bytes, service=value.service_bytes,
                       working_set=value.working_set_bytes, anon=anon, file=file_bytes)
        for key, amount in current.items():
            peak[key] = max(peak[key], amount)
        return current
    def record(phase, current):
        logging.getLogger('apex.backup.resources').info(
            '[APEX Backup Stage] stage=%s phase=%s rss=%s service=%s working_set=%s '
            'anon=%s file=%s elapsed=%.2f', name, phase, current['rss'], current['service'],
            current['working_set'], current['anon'], current['file'], time.monotonic() - started)
    def monitor():
        while not stopped.wait(0.5):
            sample()
    record('start', sample())
    observer = threading.Thread(target=monitor, daemon=True, name='backup-resource-sampler')
    observer.start()
    try:
        yield
    finally:
        stopped.set()
        observer.join(timeout=1)
        sample()
        record('peak', peak)
        for path in paths:
            discard_file_cache(path, sync=True)
        record('end', sample())
