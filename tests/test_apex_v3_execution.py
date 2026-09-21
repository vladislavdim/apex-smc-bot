from __future__ import annotations

from datetime import datetime, timezone
import unittest

from apex.domain.enums import Direction
from apex.execution.accounting import LedgerFill, actual_accounting


NOW = datetime.now(timezone.utc)


def fill(order_id: str, role: str, price: float, quantity: float, fee: float = 0.0) -> LedgerFill:
    return LedgerFill(order_id, role, price, quantity, fee, NOW)


class ApexV3ExecutionTests(unittest.TestCase):
    def test_partial_exit_uses_exchange_remainder_not_fixed_half(self):
        result = actual_accounting(
            [fill("entry", "ENTRY", 100, 10, 1), fill("tp1", "PARTIAL_EXIT", 110, 3, 0.3)],
            direction=Direction.LONG,
            initial_stop=90,
            exchange_position_quantity=7,
        )
        self.assertEqual(result.remaining_quantity, 7)
        self.assertEqual(result.remaining_fraction, 0.7)
        self.assertEqual(result.gross_pnl_quote, 30)
        self.assertAlmostEqual(result.net_r, 28.7 / 100)

    def test_weighted_entries_and_exits_include_fees_and_funding(self):
        result = actual_accounting(
            [
                fill("e1", "ENTRY", 100, 4, 0.4),
                fill("e2", "ENTRY", 102, 6, 0.6),
                fill("x1", "TP", 110, 5, 0.5),
                fill("x2", "EXIT", 108, 5, 0.5),
            ],
            direction=Direction.LONG,
            initial_stop=91.2,
            exchange_position_quantity=0,
            funding_quote=2,
        )
        self.assertEqual(result.weighted_entry, 101.2)
        self.assertEqual(result.weighted_exit, 109)
        self.assertAlmostEqual(result.gross_pnl_quote, 78)
        self.assertAlmostEqual(result.net_pnl_quote, 74)
        self.assertEqual(result.remaining_fraction, 0)

    def test_short_accounting_is_symmetric(self):
        result = actual_accounting(
            [fill("entry", "ENTRY", 100, 2), fill("exit", "SL", 105, 2)],
            direction=Direction.SHORT,
            initial_stop=105,
            exchange_position_quantity=0,
        )
        self.assertEqual(result.gross_pnl_quote, -10)
        self.assertEqual(result.net_r, -1)

    def test_requested_entry_without_fill_cannot_create_accounting(self):
        with self.assertRaisesRegex(ValueError, "confirmed_entry_fill_required"):
            actual_accounting(
                [], direction=Direction.LONG, initial_stop=90,
                exchange_position_quantity=0,
            )


if __name__ == "__main__":
    unittest.main()
