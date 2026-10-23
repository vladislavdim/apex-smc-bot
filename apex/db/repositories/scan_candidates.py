"""Persist detector results before another symbol can fail or time out."""
import hashlib
import json
import time


class ScanCandidates(list):
    def __init__(self, factory, strategy):
        self.factory, self.strategy = factory, strategy
        conn = factory()
        try:
            with conn:
                conn.execute('''CREATE TABLE IF NOT EXISTS scan_candidate_queue(
                    identity TEXT PRIMARY KEY,strategy TEXT NOT NULL,
                    payload TEXT NOT NULL,created REAL NOT NULL)''')
                conn.execute('DELETE FROM scan_candidate_queue WHERE created < ?', (time.time()-900,))
                # Upgrade pre-strategy keys atomically without losing restart work.
                rows = conn.execute('SELECT identity,payload,created FROM scan_candidate_queue WHERE strategy=?', (strategy,)).fetchall()
                for old_identity, payload, created in rows:
                    identity = self.identity(json.loads(payload))
                    if identity != old_identity:
                        conn.execute('INSERT OR IGNORE INTO scan_candidate_queue VALUES(?,?,?,?)',
                                     (identity,strategy,payload,created))
                        conn.execute('DELETE FROM scan_candidate_queue WHERE identity=? AND strategy=?', (old_identity,strategy))
                rows = conn.execute('SELECT payload FROM scan_candidate_queue WHERE strategy=? ORDER BY created', (strategy,)).fetchall()
            super().__init__(json.loads(row[0]) for row in rows)
        finally:
            conn.close()

    def identity(self, item):
        facts = {key: item.get(key) for key in ('symbol','direction','entry','sl','tp','tp1','tp2','snapshot_id')}
        facts['strategy'] = self.strategy
        return hashlib.sha256(json.dumps(facts, sort_keys=True, default=str).encode()).hexdigest()

    def append(self, item):
        payload = json.dumps(item, default=str)
        if len(payload.encode()) > 131072:
            raise ValueError('scan_candidate_payload_too_large')
        identity = self.identity(item)
        conn = self.factory()
        try:
            with conn:
                inserted = conn.execute('INSERT OR IGNORE INTO scan_candidate_queue VALUES(?,?,?,?)',
                                        (identity,self.strategy,payload,time.time())).rowcount
        finally:
            conn.close()
        if inserted:
            super().append(item)

    def extend(self, items):
        for item in items:
            self.append(item)

    def retire(self, item):
        conn = self.factory()
        try:
            with conn:
                conn.execute('DELETE FROM scan_candidate_queue WHERE identity=? AND strategy=?', (self.identity(item),self.strategy))
        finally:
            conn.close()
