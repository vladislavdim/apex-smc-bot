"""Expanded deterministic WYCKOFF fixtures for lookback tests."""

from tests.wyckoff_sequence_fixture import aggregate, fixture


def expanded(kind, scale, padding):
    c = fixture(kind, "ordered", scale)
    if not padding:
        return c
    first = c["bars"][0]["open_time"]
    bars = []
    for i in range(padding * 6):
        row = dict(open=100.0, close=100.0, high=101.0, low=99.0, volume=10.0)
        row.update(
            open_time=first - (padding * 6 - i) * 14400,
            close_time=first - (padding * 6 - i - 1) * 14400,
            is_closed=True,
        )
        bars.append(row)
    if padding >= 30:
        # A much older SC/BC outside native's last61 daily bars.
        for j in range(6):
            bars[10 * 6 + j].update(
                open=105.0, close=90.0, high=106.0, low=85.0, volume=10000.0
            )
        for j in range(6):
            bars[11 * 6 + j].update(
                open=95.0, close=180.0, high=200.0, low=94.0, volume=10.0
            )
    if kind == "DISTRIBUTION":
        for r in bars:
            r.update(
                open=220 - r["open"],
                close=220 - r["close"],
                high=220 - r["low"],
                low=220 - r["high"],
            )
    for r in bars:
        for k in ("open", "high", "low", "close"):
            r[k] *= scale
    c["bars"] = bars + c["bars"]
    c["daily"] = aggregate(c["bars"])
    return c
