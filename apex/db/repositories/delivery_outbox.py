"""Recoverable immutable Telegram intents; unknown outcomes require evidence."""

from __future__ import annotations
import hashlib
import json
from contextlib import closing
from apex.db.connection import connect_compatibility


class DeliveryOutbox:
    def __init__(self, path):
        self.path = path

    def prepare(self, candidate, cache_key, pair_key, claimed_at, destinations):
        identity = hashlib.sha256(f"{cache_key}:{claimed_at}".encode()).hexdigest()[:24]
        fields = {
            "symbol",
            "direction",
            "entry",
            "sl",
            "tp",
            "tp1",
            "tp2",
            "tp3",
            "rr",
            "grade",
            "timeframe",
            "hours",
            "confluence",
            "confluence_score",
            "score",
            "regime",
            "scan_type",
            "signal_type",
            "text",
            "setup_assessment",
            "_delivery_created_at",
            "_v3_candidate_id",
            "_scan_run_id", "_v3_strategy_trace", "snapshot_id", "snapshot_as_of", "technical_evidence",
        }
        snapshot = {key: value for key, value in candidate.items() if key in fields}
        review = candidate.get("_external_quality_review")
        if isinstance(review, dict):
            snapshot["_external_quality_reviewed"] = bool(
                candidate.get("_external_quality_reviewed")
            )
            snapshot["_external_quality_review"] = {
                key: review.get(key)
                for key in ("decision", "confidence", "degraded", "reasons", "reason_codes", "short_summary")
            }
        payload = json.dumps(snapshot, sort_keys=True, default=str)
        if len(payload.encode()) > 131072:
            raise ValueError("delivery_payload_exceeds_state_budget")
        with closing(connect_compatibility(self.path)) as conn, conn:
            conn.execute(
                """INSERT INTO signal_delivery_outbox
                (delivery_id,cache_key,pair_key,claimed_at,candidate_json,destinations_json)
                VALUES(?,?,?,?,?,?)""",
                (
                    identity,
                    cache_key,
                    pair_key,
                    claimed_at,
                    payload,
                    json.dumps(destinations),
                ),
            )
        return identity

    def receipt(self, identity, destination, status, message_id=None):
        with closing(connect_compatibility(self.path)) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT receipts_json FROM signal_delivery_outbox WHERE delivery_id=?",
                (identity,),
            ).fetchone()
            if row is None:
                raise RuntimeError("delivery_outbox_missing")
            receipts = json.loads(row[0])
            receipts[str(destination)] = {"status": status, "message_id": message_id}
            conn.execute(
                "UPDATE signal_delivery_outbox SET receipts_json=?,updated_at=CURRENT_TIMESTAMP WHERE delivery_id=?",
                (json.dumps(receipts), identity),
            )

    def finish(self, identity, status):
        if status not in {"DELIVERED", "ABORTED", "UNKNOWN"}:
            raise ValueError("invalid_outbox_status")
        with closing(connect_compatibility(self.path)) as conn, conn:
            conn.execute(
                "UPDATE signal_delivery_outbox SET status=?,updated_at=CURRENT_TIMESTAMP WHERE delivery_id=?",
                (status, identity),
            )

    def pending(self):
        with closing(connect_compatibility(self.path, read_only=True)) as conn:
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM signal_delivery_outbox WHERE status IN ('PENDING','UNKNOWN') ORDER BY created_at LIMIT 100"
                )
            ]

    def get(self, identity):
        with closing(connect_compatibility(self.path, read_only=True)) as conn:
            row = conn.execute(
                "SELECT * FROM signal_delivery_outbox WHERE delivery_id=?", (identity,)
            ).fetchone()
            return dict(row) if row else None

    def resolve(self, identity, action, actor, evidence):
        if (
            action not in {"confirm", "release"}
            or not str(evidence).strip()
            or not actor
        ):
            raise ValueError("resolution_requires_action_actor_evidence")
        with closing(connect_compatibility(self.path)) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM signal_delivery_outbox WHERE delivery_id=? AND status IN ('PENDING','UNKNOWN')",
                (identity,),
            ).fetchone()
            if row is None:
                raise ValueError("unresolved_delivery_not_found")
            for key in (row["cache_key"], row["pair_key"]):
                if action == "confirm" and key == row["cache_key"]:
                    conn.execute(
                        "UPDATE delivery_claims SET delivered_at=?,updated_at=CURRENT_TIMESTAMP WHERE cache_key=? AND claimed_at=?",
                        (row["claimed_at"], key, row["claimed_at"]),
                    )
                else:
                    conn.execute(
                        "DELETE FROM delivery_claims WHERE cache_key=? AND claimed_at=?",
                        (key, row["claimed_at"]),
                    )
            conn.execute(
                "UPDATE signal_delivery_outbox SET status=?,resolution_json=?,updated_at=CURRENT_TIMESTAMP WHERE delivery_id=?",
                (
                    "DELIVERED" if action == "confirm" else "ABORTED",
                    json.dumps(
                        {"action": action, "actor": actor, "evidence": evidence}
                    ),
                    identity,
                ),
            )

    def uncertain_claims(self):
        with closing(connect_compatibility(self.path, read_only=True)) as conn:
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM delivery_claims WHERE delivered_at<0 ORDER BY claimed_at LIMIT 100"
                )
            ]

    def release_legacy_claim(self, key, timestamp, actor, evidence):
        if not actor or not str(evidence).strip():
            raise ValueError("resolution_evidence_required")
        with closing(connect_compatibility(self.path)) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute(
                "SELECT 1 FROM signal_delivery_outbox WHERE (cache_key=? OR pair_key=?) AND status IN ('PENDING','UNKNOWN')",
                (key, key),
            ).fetchone():
                raise ValueError("resolve_full_outbox_instead")
            count = conn.execute(
                "DELETE FROM delivery_claims WHERE cache_key=? AND claimed_at=? AND delivered_at<0",
                (key, float(timestamp)),
            ).rowcount
            if count != 1:
                raise ValueError("uncertain_claim_not_found")
            conn.execute(
                "INSERT INTO delivery_resolutions(cache_key,claimed_at,actor,evidence,action) VALUES(?,?,?,?,?)",
                (key, float(timestamp), str(actor), str(evidence), "release"),
            )
