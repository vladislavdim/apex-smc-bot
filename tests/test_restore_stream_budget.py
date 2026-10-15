import gzip
import hashlib
from unittest.mock import Mock, patch

import pytest

from apex.db.backup import BrainPersistence


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
