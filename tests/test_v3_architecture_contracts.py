"""Architecture contract tests for canonical APEX V3 boundaries."""
from apex.ui.telegram.app import register_handlers
from apex.ui.telegram.router import register_telegram_handlers
from apex.quality.groq_gate import geometry
from apex.domain.models import Candidate


def test_telegram_app_uses_canonical_router():
    assert register_handlers is register_telegram_handlers


def test_candidate_geometry_surface_is_immutable_contract():
    fields=Candidate.__dataclass_fields__
    assert all(name in fields for name in ("entry","initial_sl","tp1","tp2","tp3","rr"))
