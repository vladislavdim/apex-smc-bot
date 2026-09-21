"""Canonical order-submission facade."""
from core.trade_execution import execute_approved_candidate, execute_manager_review

__all__ = ["execute_approved_candidate", "execute_manager_review"]
