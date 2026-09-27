"""Application-level shutdown composition for APEX V3."""
from apex.ops.graceful_shutdown import GracefulShutdown

def coordinator(*hooks):
    shutdown=GracefulShutdown()
    for hook in hooks: shutdown.add(hook)
    return shutdown
__all__=["GracefulShutdown","coordinator"]
