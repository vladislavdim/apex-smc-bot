"""Injected legacy price-alert service without trading authority."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Awaitable, Callable


@dataclass(frozen=True)
class PriceAlertService:
    connector: Callable
    database_path: str
    price_provider: Callable[[], dict]
    sender: Callable[..., Awaitable]

    async def check(self) -> None:
        """Mark and notify alerts whose configured price boundary was crossed."""
        try:
            conn = self.connector(
                self.database_path, timeout=30, check_same_thread=False,
            )
            alerts = conn.execute(
                "SELECT id, user_id, symbol, price_level, direction "
                "FROM alerts WHERE triggered=0"
            ).fetchall()
            conn.close()

            prices = self.price_provider()
            for alert_id, user_id, symbol, level, direction in alerts:
                if symbol not in prices:
                    continue
                current = prices[symbol]["price"]
                triggered = (
                    (direction == "above" and current >= level)
                    or (direction == "below" and current <= level)
                )
                if not triggered:
                    continue

                update = self.connector(
                    self.database_path, timeout=30, check_same_thread=False,
                )
                update.execute(
                    "UPDATE alerts SET triggered=1 WHERE id=?", (alert_id,)
                )
                update.commit()
                update.close()
                arrow = "⬆️" if direction == "above" else "⬇️"
                try:
                    await self.sender(
                        user_id,
                        f"🔔 <b>АЛЕРТ СРАБОТАЛ!</b>\n\n"
                        f"{arrow} <b>{symbol}</b> достиг уровня "
                        f"<code>{level}</code>\n"
                        f"Текущая цена: <code>{current:.4f}</code>",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
        except Exception as error:
            logging.error("Alert check error: %s", error)


_service: PriceAlertService | None = None


def configure_price_alerts(service: PriceAlertService | None) -> None:
    global _service
    _service = service


async def check_alerts() -> None:
    """Run the configured UI alert service; fail closed before composition."""
    service = _service
    if service is not None:
        await service.check()


__all__ = ["PriceAlertService", "check_alerts", "configure_price_alerts"]
