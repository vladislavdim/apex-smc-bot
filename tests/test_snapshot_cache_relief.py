from unittest.mock import patch

from apex.db.snapshot_io import _relieve_stage_cache


def test_snapshot_cache_evicted_before_container_limit():
    sample = {'service': 425 * 1024**2, 'file': 155 * 1024**2}
    with patch('apex.db.snapshot_io.discard_file_cache') as discard:
        assert _relieve_stage_cache(sample, 512 * 1024**2, ('snapshot.db',))
        discard.assert_called_once_with('snapshot.db')


def test_snapshot_cache_kept_when_no_pressure_or_reclaimable_pages():
    with patch('apex.db.snapshot_io.discard_file_cache') as discard:
        assert not _relieve_stage_cache({'service': 350 * 1024**2, 'file': 150 * 1024**2}, 512 * 1024**2, ('snapshot.db',))
        assert not _relieve_stage_cache({'service': 450 * 1024**2, 'file': 8 * 1024**2}, 512 * 1024**2, ('snapshot.db',))
        assert not _relieve_stage_cache({'service': 450 * 1024**2, 'file': 150 * 1024**2}, 0, ('snapshot.db',))
        discard.assert_not_called()
