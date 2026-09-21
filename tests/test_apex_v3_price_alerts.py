import sqlite3
import tempfile
import unittest

from apex.ui.price_alerts import PriceAlertService, check_alerts, configure_price_alerts


class PriceAlertServiceTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self):
        configure_price_alerts(None)

    async def test_crossed_boundaries_are_marked_and_sent_once(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as handle:
            conn = sqlite3.connect(handle.name)
            conn.execute(
                "CREATE TABLE alerts(id INTEGER PRIMARY KEY,user_id INTEGER,"
                "symbol TEXT,price_level REAL,direction TEXT,triggered INTEGER)"
            )
            conn.executemany(
                "INSERT INTO alerts VALUES(?,?,?,?,?,0)",
                ((1, 7, "BTCUSDT", 100, "above"),
                 (2, 8, "ETHUSDT", 50, "below"),
                 (3, 9, "SOLUSDT", 20, "above")),
            )
            conn.commit()
            conn.close()
            sent = []

            async def sender(user_id, text, **kwargs):
                sent.append((user_id, text, kwargs))

            configure_price_alerts(PriceAlertService(
                sqlite3.connect,
                handle.name,
                lambda: {
                    "BTCUSDT": {"price": 101},
                    "ETHUSDT": {"price": 49},
                    "SOLUSDT": {"price": 19},
                },
                sender,
            ))
            await check_alerts()

            conn = sqlite3.connect(handle.name)
            states = conn.execute(
                "SELECT id,triggered FROM alerts ORDER BY id"
            ).fetchall()
            conn.close()
            self.assertEqual(states, [(1, 1), (2, 1), (3, 0)])
            self.assertEqual([item[0] for item in sent], [7, 8])
            self.assertTrue(all(item[2] == {"parse_mode": "HTML"} for item in sent))

    async def test_unconfigured_service_is_a_safe_noop(self):
        configure_price_alerts(None)
        self.assertIsNone(await check_alerts())


if __name__ == "__main__":
    unittest.main()
