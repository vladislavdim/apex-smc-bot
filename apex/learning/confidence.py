"""Sample-size labels required by APEX V3."""


def confidence_label(samples: int) -> str:
    count = max(0, int(samples))
    if count < 10:
        return "INSUFFICIENT"
    if count < 30:
        return "LOW"
    if count < 100:
        return "MEDIUM"
    return "STRONGER"


__all__ = ["confidence_label"]
