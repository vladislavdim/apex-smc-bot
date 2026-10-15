import gzip
import hashlib
import sqlite3
from unittest.mock import Mock, patch

import pytest

from apex.db.backup import BrainPersistence
from apex.db.snapshot_io import sqlite_snapshot


@pytest.mark.parametrize('compressed', [False, True])
def test_restore_writes_in_chunks_and_flushes_during_large_download(tmp_path, compressed):
    raw = b'apex-snapshot-record\n' * 600000
    payload = gzip.compress(raw) if compressed else raw
    manager = BrainPersistence(str(tmp_path / 'brain.db'), 'owner/repo', 'test', session=Mock())
    class Target:
        def __init__(self):
            self.digest = hashlib.sha256()
            self.largest = self.flushes = 0
        def write(self, chunk):
            self.largest = max(self.largest, len(chunk))
            self.digest.update(chunk)
        def flush(self): self.flushes += 1
        def fileno(self): return 42
    target = Target()
    with patch('apex.db.snapshot_io.os.fdatasync') as sync:
        manager._write_downloaded_snapshot(payload, target)
    assert target.digest.digest() == hashlib.sha256(raw).digest()
    assert target.largest <= 1024 * 1024
    assert target.flushes >= 1 and sync.called


def test_restore_rejects_truncated_gzip(tmp_path):
    manager = BrainPersistence(str(tmp_path / 'brain.db'), 'owner/repo', 'test', session=Mock())
    with (tmp_path / 'restore.db').open('wb') as target, pytest.raises(EOFError):
        manager._write_downloaded_snapshot(gzip.compress(b'record' * 1000)[:-5], target)


def test_snapshot_pins_wal_read_view_while_live_writes_continue(tmp_path):
    path = str(tmp_path / 'source.db')
    destination = str(tmp_path / 'copy.db')
    source = sqlite3.connect(path)
    writer = sqlite3.connect(path)
    target = sqlite3.connect(destination)
    try:
        source.execute('PRAGMA journal_mode=WAL')
        source.execute('CREATE TABLE records (id INTEGER PRIMARY KEY, version INTEGER, payload BLOB)')
        source.executemany('INSERT INTO records VALUES (?,0,?)', ((i, b'x' * 3000) for i in range(7000)))
        source.commit()
        writes = 0
        def concurrent_write(cache_path, **kwargs):
            nonlocal writes
            if cache_path == path and writes < 5:
                writes += 1
                writer.execute('UPDATE records SET version=? WHERE id=0', (writes,))
                writer.commit()
        with patch('apex.db.snapshot_io.discard_file_cache', side_effect=concurrent_write):
            sqlite_snapshot(source, target, path, destination)
        assert writes >= 2
        assert target.execute('SELECT version FROM records WHERE id=0').fetchone()[0] == 0
        assert source.execute('SELECT version FROM records WHERE id=0').fetchone()[0] == writes
        assert target.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
        assert target.execute('SELECT COUNT(*) FROM records').fetchone()[0] == 7000
    finally:
        target.close()
        writer.close()
        source.close()
