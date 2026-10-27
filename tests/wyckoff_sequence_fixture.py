"""Deterministic coherent WYCKOFF OHLCV fixtures."""

END = 1_792_972_800
MODES = (
    "ordered",
    "confirmation_before_trigger",
    "same_bar",
    "no_trigger",
    "no_confirmation",
)


def aggregate(rows):
    out = []
    for i in range(0, len(rows), 6):
        g = rows[i : i + 6]
        assert len(g) == 6
        out.append(
            dict(
                open=g[0]["open"],
                close=g[-1]["close"],
                high=max(x["high"] for x in g),
                low=min(x["low"] for x in g),
                volume=sum(x["volume"] for x in g),
                open_time=g[0]["open_time"],
                close_time=g[-1]["close_time"],
                is_closed=True,
            )
        )
    return out


def fixture(kind="SPRING", mode="ordered", scale=1.0):
    days = [
        dict(
            open=100.0,
            close=100.0,
            high=101.0,
            low=99.0,
            volume=60.0 if i < 43 else 30.0,
        )
        for i in range(50)
    ]
    days[5]["high"] = 150.0
    days[20] = dict(open=105.0, close=97.0, high=106.0, low=95.0, volume=600.0)
    days[21] = dict(open=98.0, close=112.0, high=114.0, low=96.0, volume=60.0)
    days[22] = dict(open=100.0, close=101.0, high=103.0, low=95.5, volume=48.0)
    bars = []
    for d in days:
        for j in range(6):
            o = d["open"] + (d["close"] - d["open"]) * j / 6
            c = d["open"] + (d["close"] - d["open"]) * (j + 1) / 6
            bars.append(
                dict(
                    open=o,
                    close=c,
                    high=max(o, c),
                    low=min(o, c),
                    volume=d["volume"] / 6,
                )
            )
        bars[-6]["high"] = d["high"]
        bars[-1]["low"] = d["low"]
    for b in bars:
        if b["open"] == b["close"] == b["high"] == b["low"] == 100.0:
            b.update(high=101.0, low=99.0)
    tail = [
        (100, 99, 101, 94, 5),
        (99, 112, 113, 98, 5),
        (112, 115, 116, 111, 15),
        (115, 108, 116, 107, 5),
        (108, 105, 109, 104, 5),
        (105, 110, 111, 104, 5),
    ]
    for b, (o, c, h, l, v) in zip(bars[-6:], tail):
        b.update(open=o, close=c, high=h, low=l, volume=v)
    trigger, confirmation = 294, 296
    if mode == "confirmation_before_trigger":
        bars[294], bars[296] = bars[296], bars[294]
        trigger, confirmation = 296, 294
    elif mode == "same_bar":
        bars[294] = dict(open=100.0, close=115.0, high=116.0, low=94.0, volume=15.0)
        bars[296] = dict(open=100.0, close=100.0, high=101.0, low=99.0, volume=5.0)
        trigger = confirmation = 294
    elif mode == "no_trigger":
        bars[294] = dict(open=100.0, close=100.0, high=101.0, low=99.0, volume=5.0)
    elif mode == "no_confirmation":
        bars[296] = dict(open=100.0, close=100.0, high=101.0, low=99.0, volume=5.0)
    for i, b in enumerate(bars):
        b.update(
            open_time=END - (300 - i) * 14400,
            close_time=END - (299 - i) * 14400,
            is_closed=True,
        )
    if kind == "DISTRIBUTION":
        for b in bars:
            b.update(
                open=220 - b["open"],
                close=220 - b["close"],
                high=220 - b["low"],
                low=220 - b["high"],
            )
    for b in bars:
        for k in ("open", "close", "high", "low"):
            b[k] *= scale
    daily = aggregate(bars)
    oracle = dict(
        climax_close=daily[20]["close_time"],
        reaction_close=daily[21]["close_time"],
        test_close=daily[22]["close_time"],
        trigger_close=bars[trigger]["close_time"],
        confirmation_close=bars[confirmation]["close_time"],
        expected_sequence=mode == "ordered",
    )
    return dict(
        kind=kind, mode=mode, scale=scale, daily=daily, bars=bars, oracle=oracle
    )
