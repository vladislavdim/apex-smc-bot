"""Canonical read-only UI API projection registry."""
from apex.ui.readmodels import execution,health,learning,manager,market,overview,strategies,trades
PROJECTORS={"overview":overview.project,"strategies":strategies.project,"trades":trades.project,"manager":manager.project,"execution":execution.project,"market":market.project,"learning":learning.project,"health":health.project}
def project_tab(tab,payload):
    try: fn=PROJECTORS[str(tab).lower()]
    except KeyError as exc: raise ValueError("unknown_UI_tab") from exc
    return fn(payload)
__all__=["PROJECTORS","project_tab"]
