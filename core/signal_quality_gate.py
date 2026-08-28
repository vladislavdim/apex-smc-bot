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
from external_sources.models import empty_context
from external_sources.storage import persist_context
from news_context.aggregator import collect_news_context, format_news_context
from news_context.storage import persist_news_context


DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db")
_VALID_DECISIONS = {"APPROVE", "WAIT", "REJECT"}


def _candidate_view(candidate: dict[str, Any]) -> dict[str, Any]:
    """Whitelist fields; AI must not receive or mutate the Telegram message."""
    evidence = candidate.get("technical_evidence")
    if not isinstance(evidence, dict):
        evidence = {
            key: candidate.get(key) for key in (
                "logic", "htf_dir", "htf_1w", "weekly_warning", "confirms",
                "zone", "zone_type", "q_score", "phases", "spring", "sos",
                "direction_1d", "ob", "fvg",
            ) if candidate.get(key) is not None
        }
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
        "technical_evidence": evidence,
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


def _persist_review(candidate: dict[str, Any], context: dict[str, Any], news: dict[str, Any], review: dict[str, Any]) -> None:
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
            news_context_json TEXT,
            degraded INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        columns = {row[1] for row in conn.execute("PRAGMA table_info(ai_signal_reviews)")}
        if "news_context_json" not in columns:
            conn.execute("ALTER TABLE ai_signal_reviews ADD COLUMN news_context_json TEXT")
        view = _candidate_view(candidate)
        candidate_key = hashlib.sha256(
            json.dumps(view, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()[:24]
        conn.execute(
            """INSERT INTO ai_signal_reviews
               (candidate_key, symbol, strategy, direction, timeframe, decision,
                confidence, reasons_json, risks_json, context_json, news_context_json, degraded)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                json.dumps(news, ensure_ascii=False, default=str),
                int(bool(review.get("degraded"))),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logging.warning("[SignalQualityGate] review persistence failed: %s", exc)


async def review_signal_candidate(
    candidate: dict[str, Any],
    ask_groq: Callable[..., str | None],
) -> dict[str, Any]:
    """Enrich a completed candidate and let Groq approve, wait, or reject it.

    Failures are deliberately fail-open so an unavailable external provider or
    Groq key never silently replaces the existing APEX strategy decision.
    """
    view = _candidate_view(candidate)
    context_result, news_result = await asyncio.gather(
        collect_external_context(str(candidate.get("symbol", "")), str(view.get("direction", ""))),
        collect_news_context(str(candidate.get("symbol", ""))),
        return_exceptions=True,
    )
    if isinstance(context_result, dict):
        context = context_result
    else:
        context = empty_context(str(candidate.get("symbol", "")))
        context["external_data_unavailable"] = True
        context["data_quality"]["failed_sources"] = [f"internal:{type(context_result).__name__}"]
    news = news_result if isinstance(news_result, dict) else {
        "news_data_unavailable": True,
        "risk_level": "UNKNOWN",
        "data_quality": {"available_sources": [], "failed_sources": [type(news_result).__name__]},
    }
    strategy = str(view.get("strategy") or "").upper()
    external_block = format_external_context(context, strategy)
    news_block = format_news_context(news)
    prompt = f"""You are the final quality reviewer for an already calculated crypto trade candidate.

The APEX strategy has already calculated direction, entry, SL, TP and RR.
You MUST NOT recalculate, edit or propose replacements for those values.
Evaluate the technical evidence, external positioning and fresh news risk together.
Use external/news data as contextual evidence, not as an independent signal.
Missing providers alone are not a reason to reject. A material conflict must result
in valid=false and decision=REJECT or WAIT. A scheduled critical release is volatility
risk, never proof of direction. Prefer WAIT during a high-risk release window unless
the supplied evidence specifically justifies approval. Never invent forecast or actual values.
Do not treat Ethereum-wide data as pair-specific evidence for another chain.

CANDIDATE:
{json.dumps(view, ensure_ascii=False, default=str)}

{external_block}

{news_block}

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
    try:
        min_confidence = float(os.environ.get("GROQ_MIN_APPROVAL_CONFIDENCE", "0.65"))
    except ValueError:
        min_confidence = 0.65
    min_confidence = max(0.0, min(1.0, min_confidence))
    if not review["degraded"] and review["decision"] == "APPROVE" and review["confidence"] < min_confidence:
        review["decision"] = "WAIT"
        review["risks"].append(f"Groq approval confidence below {min_confidence:.2f}")
    review["context"] = context
    review["news_context"] = news
    await asyncio.to_thread(persist_context, context, strategy, True, review.get("decision"))
    await asyncio.to_thread(persist_news_context, news, strategy, review.get("decision"))
    await asyncio.to_thread(_persist_review, candidate, context, news, review)
    return review
