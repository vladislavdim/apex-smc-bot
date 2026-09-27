"""Application shutdown composition for APEX V3.

Application code registers concrete persistence/transport hooks here while the
ops layer supplies the idempotent shutdown mechanism.
"""
from apex.ops.graceful_shutdown import GracefulShutdown

def build_shutdown(*hooks):
    coordinator=GracefulShutdown()
    for hook in hooks: coordinator.add(hook)
    return coordinator
__all__=["GracefulShutdown","build_shutdown"]
