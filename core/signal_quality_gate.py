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
from core.htf_close_context import build_htf_close_context, format_htf_close_context
from core.setup_evidence import assess_candidate, persist_assessment
from core.setup_audit import emit_groq_review_event as _emit_setup_audit_groq

try:
    from .market_memory import build_memory_context, format_market_memory_context
    from .historical_zones import build_zone_context, format_zone_context
    from .outcome_learning import build_learning_context, format_learning_context
except ImportError:  # market.py also supports loading core/ as a direct module path
    from market_memory import build_memory_context, format_market_memory_context
    from historical_zones import build_zone_context, format_zone_context
    from outcome_learning import build_learning_context, format_learning_context


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
            "decision": "WAIT",
            "confidence": 0.0,
            "reasons": ["Groq review unavailable; final confirmation required"],
            "risks": [],
            "degraded": True,
            "raw": raw or "",
        }
    decision = str(data.get("decision", "WAIT")).upper()
    if decision not in _VALID_DECISIONS:
        decision = "WAIT"
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


def _persist_review(
    candidate: dict[str, Any],
    context: dict[str, Any],
    news: dict[str, Any],
    memory: dict[str, Any],
    zones: dict[str, Any],
    learning: dict[str, Any],
    review: dict[str, Any],
) -> None:
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
            market_memory_json TEXT,
            historical_zones_json TEXT,
            closed_loop_json TEXT,
            degraded INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        columns = {row[1] for row in conn.execute("PRAGMA table_info(ai_signal_reviews)")}
        if "news_context_json" not in columns:
            conn.execute("ALTER TABLE ai_signal_reviews ADD COLUMN news_context_json TEXT")
        if "market_memory_json" not in columns:
            conn.execute("ALTER TABLE ai_signal_reviews ADD COLUMN market_memory_json TEXT")
        if "historical_zones_json" not in columns:
            conn.execute("ALTER TABLE ai_signal_reviews ADD COLUMN historical_zones_json TEXT")
        if "closed_loop_json" not in columns:
            conn.execute("ALTER TABLE ai_signal_reviews ADD COLUMN closed_loop_json TEXT")
        view = _candidate_view(candidate)
        candidate_key = hashlib.sha256(
            json.dumps(view, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()[:24]
        conn.execute(
            """INSERT INTO ai_signal_reviews
               (candidate_key, symbol, strategy, direction, timeframe, decision,
                confidence, reasons_json, risks_json, context_json, news_context_json,
                market_memory_json, historical_zones_json, closed_loop_json, degraded)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                json.dumps(memory, ensure_ascii=False, default=str),
                json.dumps(zones, ensure_ascii=False, default=str),
                json.dumps(learning, ensure_ascii=False, default=str),
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
    candle_loader: Callable[[str, str, int], list] | None = None,
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
    memory = await asyncio.to_thread(
        build_memory_context,
        str(view.get("symbol") or ""), strategy,
        str(view.get("direction") or ""), str(view.get("timeframe") or ""),
    )
    zones = await asyncio.to_thread(build_zone_context, str(view.get("symbol") or ""), view.get("entry"), str(view.get("timeframe") or ""))
    evidence_candidate = dict(candidate)
    evidence_candidate["_external_quality_review"] = {"context": context, "news_context": news, "historical_zones": zones}
    learning = await asyncio.to_thread(build_learning_context, evidence_candidate)
    htf_close = await asyncio.to_thread(
        build_htf_close_context, str(view.get("symbol") or ""), strategy, candle_loader
    )
    setup_assessment = assess_candidate(candidate, context)
    external_block = format_external_context(context, strategy)
    news_block = format_news_context(news)
    memory_block = format_market_memory_context(memory)
    zone_block = format_zone_context(zones)
    learning_block = format_learning_context(learning)
    htf_close_block = format_htf_close_context(htf_close)
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
Historical zones describe prior reactions only and MUST NOT replace the already
calculated entry, SL or TP. Closed-loop evidence is statistical context, not
permission to invent a new strategy. Do not propose or activate a new strategy
unless new_strategy_research_ready=true.
Validated experience rules inside technical_evidence are out-of-sample observations.
An ACTIVE or PROBATION AVOID rule that directly matches this strategy and regime may
reduce confidence or justify WAIT; a CONFIRM rule may support existing evidence but
can never approve a candidate by itself. These rules MUST NOT alter any price level.
Weekly/monthly closed-candle context is background HTF evidence only. It may adjust
confidence or be listed as a risk, but a conflicting weekly/monthly candle alone is
NOT sufficient to WAIT or REJECT an otherwise valid candidate.
The deterministic SETUP EVIDENCE assessment is authoritative about causal completeness.
You may downgrade its class, but you MUST NOT promote INVALID or DEVELOPING to APPROVE.
Correlated observations inside one domain are one body of evidence, not multiple votes.

CANDIDATE:
{json.dumps(view, ensure_ascii=False, default=str)}

SETUP EVIDENCE (deterministic, read-only levels):
{json.dumps(setup_assessment, ensure_ascii=False, default=str)}

{external_block}

{news_block}

{memory_block}

{zone_block}

{learning_block}

{htf_close_block}

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

    parsed = _extract_json(raw)
    if raw is not None and parsed is None:
        # The provider answered but the final text was not valid JSON. Retry once
        # with a compact immutable candidate/assessment contract. This retry is
        # format recovery only; it cannot alter APEX-calculated trade levels.
        compact_view = {
            key: view.get(key) for key in (
                "symbol", "strategy", "direction", "timeframe", "entry", "sl",
                "tp1", "tp2", "tp3", "rr", "confluence_score", "regime",
            )
        }
        compact_assessment = {
            key: setup_assessment.get(key) for key in (
                "state", "class", "fatal_reasons", "missing", "warnings",
                "trigger_ready", "geometry_ready", "causal_domains",
            ) if setup_assessment.get(key) is not None
        }
        compact_prompt = f"""You are the final quality reviewer for an already calculated crypto trade candidate.
Do NOT recalculate or replace entry, SL, TP or RR.
The deterministic SETUP EVIDENCE assessment is authoritative: INVALID cannot be approved; DEVELOPING cannot be approved.

CANDIDATE:
{json.dumps(compact_view, ensure_ascii=False, default=str)}

SETUP EVIDENCE:
{json.dumps(compact_assessment, ensure_ascii=False, default=str)}

Return JSON only, with no markdown or commentary:
{{
  \"valid\": true,
  \"decision\": \"APPROVE|WAIT|REJECT\",
  \"confidence\": 0.0,
  \"reasons\": [\"specific evidence\"],
  \"risks\": [\"specific risk\"]
}}"""
        try:
            retry_raw = await asyncio.wait_for(
                asyncio.to_thread(ask_groq, compact_prompt, 350),
                timeout=35,
            )
            retry_parsed = _extract_json(retry_raw)
            if retry_parsed is not None:
                raw = retry_raw
                parsed = retry_parsed
                logging.info("[SignalQualityGate] recovered malformed Groq review with compact JSON retry")
            else:
                logging.warning("[SignalQualityGate] Groq parse error after compact retry")
        except Exception as exc:
            logging.warning("[SignalQualityGate] compact Groq retry unavailable: %s", exc)

    review = _normalize_review(parsed, raw)
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
    matrix_ready = bool((candidate.get("technical_evidence") or {}).get("causal_matrix_ready"))
    if matrix_ready and setup_assessment.get("state") == "INVALID":
        review["decision"] = "REJECT"
        review["reasons"].append("Deterministic setup evidence: INVALID")
    elif matrix_ready and setup_assessment.get("state") == "DEVELOPING":
        review["decision"] = "WAIT"
        review["reasons"].append("Deterministic setup evidence: causal chain is still DEVELOPING")
    review["setup_assessment"] = setup_assessment
    review["context"] = context
    review["news_context"] = news
    review["market_memory"] = memory
    review["historical_zones"] = zones
    review["closed_loop_learning"] = learning
    await asyncio.to_thread(persist_context, context, strategy, True, review.get("decision"))
    await asyncio.to_thread(persist_news_context, news, strategy, review.get("decision"))
    await asyncio.to_thread(_persist_review, candidate, context, news, memory, zones, learning, review)
    await asyncio.to_thread(_emit_setup_audit_groq, candidate, review)
    if matrix_ready:
        await asyncio.to_thread(persist_assessment, candidate, setup_assessment, "FINAL", DB_PATH)
    return review
