"""Canonical APEX V3 operational constants.

Keep environment parsing in settings.py; this module contains only stable names
shared across layers.
"""
PRODUCTION_STRATEGIES=("FAST","MTF","ZONE","SWING","WYCKOFF")
DASHBOARD_TABS=("overview","strategies","trades","manager","execution","market","learning","health")
__all__=["PRODUCTION_STRATEGIES","DASHBOARD_TABS"]
