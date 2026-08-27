"""Groq quality gate applied only after an APEX strategy finds a candidate."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
from typing import Any, Callable

from external_sources.aggregator import collect_external_context, format_external_context
from external_sources.storage import persist_context


DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db")
_VALID_DECISIONS = {"APPROVE", "WAIT", "REJECT"}


def _candidate_view(candidate: dict[str, Any]) -> dict[str, Any]:
    """Whitelist fields; AI must not receive or mutate the Telegram message."""
    return {
        "symbol": candidate.get("symbol"),
        "strategy": candidate.get("scan_type") or candidate.get("grade") or candidate.get("signal_type"),
        "direction": candidate.get("direction"),
        "timeframe": candidate.get("timeframe"),
        "entry": candidate.get("entry"),
        "sl": candidate.get("sl"),
        "tp1": candidate.get("tp1") or candidate.get("tp"),
        "tp2": candidate.get("tp2"),
        "tp3": candidate.get("tp3"),
        "rr": candidate.get("rr"),
        "confluence_score": candidate.get("confluence_score") or candidate.get("score"),
        "regime": candidate.get("regime"),
    }


def _extract_json(raw: str | None) -> dict[str, Any] | None:
    if not raw:
        return None
    clean = re.sub(r"```(?:json)?|```", "", raw, flags=re.IGNORECASE).strip()
    start, end = clean.find("{"), clean.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        parsed = json.loads(clean[start : end + 1])
        return parsed if isinstance(parsed, dict) else None
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def _normalize_review(data: dict[str, Any] | None, raw: str | None) -> dict[str, Any]:
    if not data:
        return {
            "decision": "APPROVE",
            "confidence": 0.0,
            "reasons": ["Groq review unavailable; existing APEX decision preserved"],
            "risks": [],
            "degraded": True,
            "raw": raw or "",
        }
    decision = str(data.get("decision", "APPROVE")).upper()
    if decision not in _VALID_DECISIONS:
        decision = "APPROVE"
    try:
        confidence = max(0.0, min(1.0, float(data.get("confidence", 0.0))))
    except (TypeError, ValueError):
        confidence = 0.0
    reasons = data.get("reasons") if isinstance(data.get("reasons"), list) else []
    risks = data.get("risks") if isinstance(data.get("risks"), list) else []
    return {
        "decision": decision,
        "confidence": confidence,
        "reasons": [str(item)[:300] for item in reasons[:5]],
        "risks": [str(item)[:300] for item in risks[:5]],
        "degraded": False,
        "raw": raw or "",
    }


def _persist_review(candidate: dict[str, Any], context: dict[str, Any], review: dict[str, Any]) -> None:
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""CREATE TABLE IF NOT EXISTS ai_signal_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_key TEXT,
            symbol TEXT,
            strategy TEXT,
            direction TEXT,
            timeframe TEXT,
            decision TEXT,
            confidence REAL,
            reasons_json TEXT,
            risks_json TEXT,
            context_json TEXT,
            degraded INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        view = _candidate_view(candidate)
        candidate_key = hashlib.sha256(
            json.dumps(view, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()[:24]
        conn.execute(
            """INSERT INTO ai_signal_reviews
               (candidate_key, symbol, strategy, direction, timeframe, decision,
                confidence, reasons_json, risks_json, context_json, degraded)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                candidate_key,
                view.get("symbol"),
                view.get("strategy"),
                view.get("direction"),
                view.get("timeframe"),
                review.get("decision"),
                review.get("confidence", 0.0),
                json.dumps(review.get("reasons", []), ensure_ascii=False),
                json.dumps(review.get("risks", []), ensure_ascii=False),
                json.dumps(context, ensure_ascii=False, default=str),
                int(bool(review.get("degraded"))),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.warning("[SignalQualityGate] review persistence failed: %s", exc)


def mark_candidate_not_sent(candidate: dict[str, Any], decision: str) -> None:
    """Keep an AI-rejected candidate out of pending trade monitoring.

    Several legacy scanners persist a candidate immediately before calling the
    sender.  Updating only that newest matching pending row preserves its audit
    history while preventing it from being treated as a live trade.
    """
    status = "ai_wait" if str(decision).upper() == "WAIT" else "ai_rejected"
    try:
        view = _candidate_view(candidate)
        conn = sqlite3.connect(DB_PATH, timeout=20, check_same_thread=False)
        row = conn.execute(
            """SELECT id FROM signals
               WHERE symbol=? AND direction=? AND result='pending'
               ORDER BY id DESC LIMIT 1""",
            (view.get("symbol"), view.get("direction")),
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE signals SET result=?, closed_at=CURRENT_TIMESTAMP WHERE id=?",
                (status, row[0]),
            )
            conn.commit()
        conn.close()
    except Exception as exc:
        logging.warning("[SignalQualityGate] candidate status update failed: %s", exc)


async def review_signal_candidate(
    candidate: dict[str, Any],
    ask_groq: Callable[..., str | None],
) -> dict[str, Any]:
    """Enrich a completed candidate and let Groq approve, wait, or reject it.

    Failures are deliberately fail-open so an unavailable external provider or
    Groq key never silently replaces the existing APEX strategy decision.
    """
    view = _candidate_view(candidate)
    context = await collect_external_context(str(candidate.get("symbol", "")), str(view.get("direction", "")))
    strategy = str(view.get("strategy") or "").upper()
    external_block = format_external_context(context, strategy)
    prompt = f"""You are the final quality reviewer for an already calculated crypto trade candidate.

The APEX strategy has already calculated direction, entry, SL, TP and RR.
You MUST NOT recalculate, edit or propose replacements for those values.
Use external data as contextual evidence, not as fixed hard-coded veto rules.
Judge whether derivatives positioning and smart-money context support the candidate,
contradict it strongly, or require waiting. Missing providers alone are not a reason
to reject. A material conflict must result in valid=false and decision=REJECT or WAIT.
Do not treat Ethereum-wide data as pair-specific evidence for another chain.

CANDIDATE:
{json.dumps(view, ensure_ascii=False, default=str)}

{external_block}

Return JSON only:
{{
  "valid": true,
  "decision": "APPROVE|WAIT|REJECT",
  "confidence": 0.0,
  "reasons": ["specific evidence"],
  "risks": ["specific risk"]
}}"""

    try:
        raw = await asyncio.wait_for(
            asyncio.to_thread(ask_groq, prompt, 350),
            timeout=35,
        )
    except Exception as exc:
        logging.warning("[SignalQualityGate] Groq unavailable: %s", exc)
        raw = None

    review = _normalize_review(_extract_json(raw), raw)
    parsed = _extract_json(raw)
    if parsed and parsed.get("valid") is False and review["decision"] == "APPROVE":
        review["decision"] = "REJECT"
    review["context"] = context
    await asyncio.to_thread(persist_context, context, strategy, True, review.get("decision"))
    await asyncio.to_thread(_persist_review, candidate, context, review)
    return review
