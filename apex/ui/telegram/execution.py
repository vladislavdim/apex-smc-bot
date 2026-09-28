"""Execution/account Telegram presentation."""
from apex.ui.telegram.formatters import money
def account_line(wallet=None,available=None): return f"Wallet {money(wallet)} · Available {money(available)}"
__all__=["account_line"]
