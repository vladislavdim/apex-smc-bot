"""Canonical V3 Binance USD-M execution for approved APEX candidates.

The module is deliberately isolated from strategy calculations.  It consumes
an immutable, already reviewed candidate and either records a paper order or
submits an exchange-compatible limit entry.  Live execution is disabled by
default and requires two independent environment switches.
"""

from __future__ import annotations

import hashlib
import hmac
import calendar
import logging
import re
import sqlite3
from apex.db.connection import connect_compatibility as _connect_compatibility_db
from apex.db.repositories.execution_account import ExecutionAccountRepository
from apex.db.repositories.executions import ExecutionRepository
from apex.db.repositories.manager import ManagerRepository
from apex.db.repositories.signals import SignalLifecycleRepository
from apex.db.execution_recovery import append_recovery, replay_recovery
from apex.domain.ids import derived_id, is_id
from apex.domain.enums import Direction, Strategy
from apex.domain.models import Candidate
from apex.execution.plan import client_order_ids
from apex.risk.engine import RiskLimits, RiskState, decide_risk
import threading
import time
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Mapping
from urllib.parse import urlencode
from apex.config.settings import ApexConfig

try:
    import requests
except ImportError:  # pure sizing/paper tests do not need an HTTP package
    requests = None
