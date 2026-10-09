import subprocess

from apex.db.git_snapshot import push_snapshot


def git(directory, *args):
    return subprocess.check_output(["git", "-C", str(directory), *args]).decode().strip()


def test_native_snapshot_push_preserves_other_files_and_rejects_foreign_blob(tmp_path):
    remote = tmp_path / "remote.git"
    source = tmp_path / "source"
    git(tmp_path, "init", "--bare", str(remote))
    git(tmp_path, "init", "-b", "backups", str(source))
    git(source, "config", "user.name", "test")
    git(source, "config", "user.email", "test@example.test")
    (source / "brain.db").write_bytes(b"old snapshot")
    (source / "other.db").write_bytes(b"keep this")
    git(source, "add", ".")
    git(source, "commit", "-m", "initial")
    git(source, "push", str(remote), "backups")
    original = git(source, "rev-parse", "HEAD:brain.db")
    snapshot = tmp_path / "snapshot.gz"
    snapshot.write_bytes(b"new compressed snapshot")
    arguments = dict(remote_url=str(remote), branch="backups", remote_name="brain.db",
                     snapshot_path=str(snapshot), expected_blob_sha=original, message="backup")
    result = push_snapshot(**arguments)
    assert result.status_code == 201
    assert git(remote, "show", "backups:brain.db") == "new compressed snapshot"
    assert git(remote, "show", "backups:other.db") == "keep this"
    head = git(remote, "rev-parse", "backups")
    snapshot.write_bytes(b"stale writer must never win")
    assert push_snapshot(**arguments).status_code == 409
    assert git(remote, "rev-parse", "backups") == head
